/*
   Copyright 2026 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

/* Gradual btree shrinking on the master, one small unit of work at a time. */

#include <inttypes.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bdb_int.h"
#include "sys_wrap.h"
#include <build/db.h>
#include <logmsg.h>
#include <epochlib.h>

int gbl_btree_shrink_min_file_mb = 100;
int gbl_btree_shrink_min_free_pct = 10;
int gbl_btree_shrink_moves_per_txn = 16;
int gbl_btree_shrink_plan_max_pages = 1000000;
int gbl_btree_shrink_verbose = 0;
int gbl_btree_shrink_btree_budget_sec = 30; /* then give other btrees a turn (0: never) */
int gbl_btree_shrink_trim = 1;
int gbl_btree_shrink_trim_pages_per_txn = 256;

extern int gbl_btree_shrink_debug_abort;
extern int gbl_btree_shrink_trim_debug_abort;
extern int log_delete_is_stopped(void);
extern int gbl_truncating_log;
int bdb_get_first_logfile(bdb_state_type *bdb_state, int *bdberr);
int bdb_get_last_logfile(bdb_state_type *bdb_state, int *bdberr);
void logdelete_lock(const char *func, int line);
void logdelete_unlock(const char *func, int line);

/* Meta LSN of btrees already sorted or found ineligible, to skip re-walks */
#define SHRINK_SEEN_SIZE 4096
struct shrink_seen {
    u_int8_t fileid[DB_FILE_ID_LEN];
    DB_LSN lsn;
    int used;
};

struct bdb_shrink_ctx {
    char table[MAXTABLELEN + 1];
    int file; /* position of the current btree within the table */
    char desc[64];
    struct __db_flsort *plan;
    int busy;                 /* consecutive busy steps on this btree */
    int visit_start;          /* epoch ms when work on this btree began, or 0 */
    int trimming;             /* plan is sorted; trimming the free tail */
    struct shrink_seen seen[SHRINK_SEEN_SIZE];
};

static struct shrink_seen *shrink_seen_find(struct bdb_shrink_ctx *ctx, const u_int8_t *fileid, int create)
{
    uint32_t h = 2166136261u;
    for (int i = 0; i < DB_FILE_ID_LEN; i++)
        h = (h ^ fileid[i]) * 16777619u;
    for (int i = 0; i < SHRINK_SEEN_SIZE; i++) {
        struct shrink_seen *s = &ctx->seen[(h + i) % SHRINK_SEEN_SIZE];
        if (s->used && memcmp(s->fileid, fileid, DB_FILE_ID_LEN) == 0)
            return s;
        if (!s->used) {
            if (!create)
                return NULL;
            memcpy(s->fileid, fileid, DB_FILE_ID_LEN);
            s->used = 1;
            return s;
        }
    }
    return NULL;
}

static void shrink_seen_set(struct bdb_shrink_ctx *ctx, DB *dbp, DB_LSN *lsn)
{
    struct shrink_seen *s = shrink_seen_find(ctx, dbp->fileid, 1);
    if (s)
        s->lsn = *lsn;
}

/* Counters and current target, published for 'shrink status' */
static pthread_mutex_t shrink_stats_lk = PTHREAD_MUTEX_INITIALIZER;
static struct {
    uint64_t plans;
    uint64_t ineligible;
    uint64_t unchanged;
    uint64_t moves;
    uint64_t busy;
    uint64_t stale;
    uint64_t aborted; /* by the debug hooks */
    uint64_t sorted;
    uint64_t rotated;
    uint64_t trimmed;
    uint64_t truncated;
    uint64_t truncate_busy;
    uint32_t trunc_waiting;       /* last truncate pass: btrees waiting on logs */
    uint32_t trunc_wait_file;     /* ... for this log to be deleted */
    uint32_t trunc_first_logfile; /* ... while this was the oldest log */
    char desc[64];
    uint64_t nfree;
    db_pgno_t last_pgno;
    uint32_t placed;
    uint32_t planned;
} shrink_stats;

static void shrink_publish(struct bdb_shrink_ctx *ctx)
{
    Pthread_mutex_lock(&shrink_stats_lk);
    if (ctx->plan) {
        strcpy(shrink_stats.desc, ctx->desc);
        __db_flsort_info(ctx->plan, &shrink_stats.nfree, &shrink_stats.last_pgno, &shrink_stats.placed,
                         &shrink_stats.planned);
    } else {
        shrink_stats.desc[0] = '\0';
    }
    Pthread_mutex_unlock(&shrink_stats_lk);
}

struct bdb_shrink_ctx *bdb_shrink_ctx_create(void)
{
    return calloc(1, sizeof(struct bdb_shrink_ctx));
}

/* On to the next btree of the table; it gets a fresh time budget */
static void shrink_next_btree(struct bdb_shrink_ctx *ctx)
{
    ctx->file++;
    ctx->visit_start = 0;
}

static void shrink_ctx_drop_plan(struct bdb_shrink_ctx *ctx)
{
    __db_flsort_destroy(ctx->plan);
    ctx->plan = NULL;
    ctx->trimming = 0;
    ctx->busy = 0;
}

/* Forget the current position, e.g. after losing mastership */
void bdb_shrink_ctx_reset(struct bdb_shrink_ctx *ctx)
{
    shrink_ctx_drop_plan(ctx);
    ctx->table[0] = '\0';
    ctx->file = 0;
    ctx->visit_start = 0;
    ctx->desc[0] = '\0';
}

void bdb_shrink_ctx_destroy(struct bdb_shrink_ctx *ctx)
{
    if (ctx == NULL)
        return;
    shrink_ctx_drop_plan(ctx);
    free(ctx);
}

/* Return the n-th btree of a table: data stripes, then blobs, then indexes */
static DB *shrink_btree(bdb_state_type *tbl, int n, char *desc, size_t desclen)
{
    int dtastripe = tbl->attr->dtastripe ? tbl->attr->dtastripe : 1;
    int df, nstripes, ix;

    for (df = 0; df < tbl->numdtafiles; df++) {
        nstripes = (df == 0 || tbl->attr->blobstripe) ? dtastripe : 1;
        if (n < nstripes) {
            snprintf(desc, desclen, "%s %s %d stripe %d", tbl->name, df ? "blob" : "data", df, n);
            return tbl->dbp_data[df][n];
        }
        n -= nstripes;
    }
    ix = n;
    if (ix < tbl->numix) {
        snprintf(desc, desclen, "%s index %d", tbl->name, ix);
        return tbl->dbp_ix[ix];
    }
    return NULL;
}

static int shrink_big_enough(DB *dbp, db_pgno_t last_pgno)
{
    return (uint64_t)(last_pgno + 1) * dbp->pgsize >= (uint64_t)gbl_btree_shrink_min_file_mb * 1024 * 1024;
}

static int shrink_eligible(DB *dbp, struct __db_flsort *plan)
{
    uint64_t nfree;
    db_pgno_t last_pgno;
    uint32_t placed, planned;

    __db_flsort_info(plan, &nfree, &last_pgno, &placed, &planned);
    if (last_pgno == 0 || planned == 0 || !shrink_big_enough(dbp, last_pgno))
        return 0;
    return nfree * 100 >= (uint64_t)gbl_btree_shrink_min_free_pct * last_pgno;
}

/* Too many consecutive busy steps on one btree: let the others have a turn */
#define SHRINK_MAX_BUSY 10

/* Leave this btree for now without marking it done, so it's revisited */
static void shrink_skip_btree(struct bdb_shrink_ctx *ctx)
{
    shrink_ctx_drop_plan(ctx);
    ctx->busy = 0;
    shrink_next_btree(ctx);
}

/* Done with this btree this pass: remember its meta LSN (the verified one, if given) */
static void shrink_finish_btree(struct bdb_shrink_ctx *ctx, DB *dbp, DB_LSN *verified)
{
    DB_LSN lsn;
    db_pgno_t last_pgno;

    if (verified)
        shrink_seen_set(ctx, dbp, verified);
    else if (__db_freelist_peek(dbp, &lsn, &last_pgno) == 0)
        shrink_seen_set(ctx, dbp, &lsn);
    shrink_ctx_drop_plan(ctx);
    ctx->busy = 0;
    shrink_next_btree(ctx);
}

/* One unit of work on tbl (caller holds the schema lock); 1 once its btrees are done. */
int bdb_shrink_table_step(bdb_state_type *tbl, struct bdb_shrink_ctx *ctx)
{
    bdb_state_type *bdb_state = tbl->parent ? tbl->parent : tbl;
    struct shrink_seen *seen;
    uint32_t moved = 0;
    int done = 0, plan_done = 0, rc;
    db_pgno_t last_pgno;
    DB_LSN lsn;
    DB *dbp;

    if (strcmp(ctx->table, tbl->name) != 0) {
        bdb_shrink_ctx_reset(ctx);
        strncpy(ctx->table, tbl->name, MAXTABLELEN);
    }

    if (!bdb_amimaster(bdb_state) ||
        (dbp = shrink_btree(tbl, ctx->file, ctx->desc, sizeof(ctx->desc))) == NULL) {
        bdb_shrink_ctx_reset(ctx);
        done = 1;
        goto publish;
    }

    /* Planning only reads, so no bdb lock; the caller's schema lock keeps dbp open */
    if (ctx->plan == NULL) {
        /* Avoid walking free lists which are too small or haven't changed */
        if (__db_freelist_peek(dbp, &lsn, &last_pgno) != 0 || !shrink_big_enough(dbp, last_pgno)) {
            shrink_stats.ineligible++;
            shrink_next_btree(ctx);
            goto publish;
        }
        seen = shrink_seen_find(ctx, dbp->fileid, 0);
        if (seen && seen->lsn.file == lsn.file && seen->lsn.offset == lsn.offset) {
            shrink_stats.unchanged++;
            shrink_next_btree(ctx);
            goto publish;
        }
        rc = __db_flsort_create(dbp, gbl_btree_shrink_plan_max_pages, &ctx->plan);
        if (rc == 0 && !shrink_eligible(dbp, ctx->plan)) {
            shrink_stats.ineligible++;
            shrink_seen_set(ctx, dbp, &lsn);
            shrink_ctx_drop_plan(ctx);
            shrink_next_btree(ctx);
        } else if (rc == 0) {
            if (ctx->visit_start == 0)
                ctx->visit_start = comdb2_time_epochms();
            shrink_stats.plans++;
            if (gbl_btree_shrink_verbose)
                logmsg(LOGMSG_USER, "btree_shrink: planned %s\n", ctx->desc);
        } else {
            /* Walk raced with traffic; revisit on the next pass */
            shrink_next_btree(ctx);
        }
        goto publish;
    }

    /* Give the table's other btrees a turn; this one is revisited next pass */
    if (gbl_btree_shrink_btree_budget_sec > 0 && ctx->visit_start &&
        comdb2_time_epochms() - ctx->visit_start > (int64_t)gbl_btree_shrink_btree_budget_sec * 1000) {
        shrink_stats.rotated++;
        if (gbl_btree_shrink_verbose)
            logmsg(LOGMSG_USER, "btree_shrink: %s time budget used, revisit next pass\n", ctx->desc);
        shrink_skip_btree(ctx);
        goto publish;
    }

    /* Hold the bdb lock across the moves so mastership can't change */
    BDB_READLOCK("btree_shrink");
    if (!bdb_amimaster(bdb_state)) {
        bdb_shrink_ctx_reset(ctx);
        done = 1;
        goto out;
    }
    if (ctx->trimming) {
        uint32_t trimmed = 0;
        rc = __db_fltrim_step(dbp, ctx->plan, gbl_btree_shrink_trim_pages_per_txn, &trimmed, &plan_done, &lsn);
        shrink_stats.trimmed += trimmed;
        if (rc != DB_LOCK_NOTGRANTED)
            ctx->busy = 0;
        if (rc == DB_LOCK_NOTGRANTED) {
            shrink_stats.busy++;
            if (++ctx->busy > SHRINK_MAX_BUSY)
                shrink_skip_btree(ctx);
        } else if (rc == DB_NOTFOUND && gbl_btree_shrink_trim_debug_abort) {
            shrink_stats.aborted++;
            shrink_ctx_drop_plan(ctx);
            shrink_next_btree(ctx);
        } else if (rc != 0) {
            /* Move on so a trim which keeps failing can't stall the pass */
            shrink_stats.stale++;
            if (gbl_btree_shrink_verbose)
                logmsg(LOGMSG_USER, "btree_shrink: %s trim failed rc %d, revisit next pass\n", ctx->desc, rc);
            shrink_ctx_drop_plan(ctx);
            shrink_next_btree(ctx);
        } else if (plan_done) {
            if (gbl_btree_shrink_verbose)
                logmsg(LOGMSG_USER, "btree_shrink: trimmed %s\n", ctx->desc);
            shrink_finish_btree(ctx, dbp, &lsn);
        }
        goto out;
    }

    rc = __db_flsort_step(dbp, ctx->plan, gbl_btree_shrink_moves_per_txn, &moved, &plan_done, &lsn);
    shrink_stats.moves += moved;
    if (rc != DB_LOCK_NOTGRANTED)
        ctx->busy = 0;
    if (rc == DB_LOCK_NOTGRANTED) {
        shrink_stats.busy++;
        if (++ctx->busy > SHRINK_MAX_BUSY)
            shrink_skip_btree(ctx);
    } else if (rc == DB_NOTFOUND && gbl_btree_shrink_debug_abort) {
        shrink_stats.aborted++;
        shrink_ctx_drop_plan(ctx);
    } else if (rc != 0) {
        shrink_stats.stale++;
        if (gbl_btree_shrink_verbose)
            logmsg(LOGMSG_USER, "btree_shrink: %s plan stale rc %d, rebuilding\n", ctx->desc, rc);
        shrink_ctx_drop_plan(ctx);
    } else if (plan_done) {
        shrink_stats.sorted++;
        if (gbl_btree_shrink_verbose)
            logmsg(LOGMSG_USER, "btree_shrink: sorted %s\n", ctx->desc);
        if (gbl_btree_shrink_trim)
            ctx->trimming = 1;
        else
            shrink_finish_btree(ctx, dbp, &lsn);
    }
out:
    BDB_RELLOCK();
publish:
    shrink_publish(ctx);
    return done;
}

void bdb_shrink_dump(FILE *out)
{
    Pthread_mutex_lock(&shrink_stats_lk);
    logmsgf(LOGMSG_USER, out,
            "btree_shrink: plans %" PRIu64 " ineligible %" PRIu64 " unchanged %" PRIu64 " moves %" PRIu64
            " busy %" PRIu64 " stale %" PRIu64 " aborted %" PRIu64 " sorted %" PRIu64 " rotated %" PRIu64
            " trimmed %" PRIu64 " truncated %" PRIu64 " truncate_busy %" PRIu64 "\n",
            shrink_stats.plans, shrink_stats.ineligible, shrink_stats.unchanged, shrink_stats.moves, shrink_stats.busy,
            shrink_stats.stale, shrink_stats.aborted, shrink_stats.sorted, shrink_stats.rotated, shrink_stats.trimmed,
            shrink_stats.truncated, shrink_stats.truncate_busy);
    if (shrink_stats.trunc_waiting)
        logmsgf(LOGMSG_USER, out,
                "btree_shrink: %u btree(s) waiting to truncate until log %u is deleted (oldest log %u)\n",
                shrink_stats.trunc_waiting, shrink_stats.trunc_wait_file, shrink_stats.trunc_first_logfile);
    if (shrink_stats.desc[0] == '\0')
        logmsgf(LOGMSG_USER, out, "btree_shrink: idle\n");
    else
        logmsgf(LOGMSG_USER, out, "btree_shrink: sorting %s: placed %u of %u (free %" PRIu64 " last_pgno %u)\n",
                shrink_stats.desc, shrink_stats.placed, shrink_stats.planned, shrink_stats.nfree,
                shrink_stats.last_pgno);
    Pthread_mutex_unlock(&shrink_stats_lk);
}

/* Start a truncate pass under the bdb, recovery and log-delete locks; 0 if it may run. */
int bdb_shrink_truncate_begin(bdb_state_type *bdb_state, uint32_t *first_logfile, uint32_t *cur_logfile)
{
    int bdberr, first, cur;

    Pthread_mutex_lock(&shrink_stats_lk);
    shrink_stats.trunc_waiting = 0;
    shrink_stats.trunc_wait_file = 0;
    Pthread_mutex_unlock(&shrink_stats_lk);

    BDB_READLOCK("btree_shrink_truncate");
    bdb_readlock_recovery(bdb_state);
    logdelete_lock(__func__, __LINE__);
    if (log_delete_is_stopped() || gbl_truncating_log ||
        (first = bdb_get_first_logfile(bdb_state, &bdberr)) <= 0 ||
        (cur = bdb_get_last_logfile(bdb_state, &bdberr)) <= 0) {
        bdb_shrink_truncate_end(bdb_state);
        return -1;
    }
    *first_logfile = first;
    *cur_logfile = cur;
    Pthread_mutex_lock(&shrink_stats_lk);
    shrink_stats.trunc_first_logfile = first;
    Pthread_mutex_unlock(&shrink_stats_lk);
    return 0;
}

void bdb_shrink_truncate_end(bdb_state_type *bdb_state)
{
    logdelete_unlock(__func__, __LINE__);
    bdb_unlock_recovery(bdb_state);
    BDB_RELLOCK();
}

/* Cut trimmed pages from each of a table's btrees; see __db_physical_truncate */
void bdb_shrink_truncate_table(bdb_state_type *tbl, uint32_t first_logfile, uint32_t cur_logfile)
{
    char desc[64];
    uint32_t pages, wait;
    DB *dbp;
    int rc;

    for (int n = 0; (dbp = shrink_btree(tbl, n, desc, sizeof(desc))) != NULL; n++) {
        rc = __db_physical_truncate(dbp, first_logfile, cur_logfile, &pages, &wait);
        Pthread_mutex_lock(&shrink_stats_lk);
        if (rc != 0) {
            shrink_stats.truncate_busy++;
        } else if (wait) {
            shrink_stats.trunc_waiting++;
            if (wait > shrink_stats.trunc_wait_file)
                shrink_stats.trunc_wait_file = wait;
        } else {
            shrink_stats.truncated += pages;
        }
        Pthread_mutex_unlock(&shrink_stats_lk);
        if (gbl_btree_shrink_verbose && rc != 0)
            logmsg(LOGMSG_USER, "btree_shrink: truncate %s busy rc %d\n", desc, rc);
        else if (gbl_btree_shrink_verbose && pages > 0)
            logmsg(LOGMSG_USER, "btree_shrink: truncated %u pages from %s\n", pages, desc);
    }
}
