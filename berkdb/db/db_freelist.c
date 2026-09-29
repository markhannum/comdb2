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

/* Free-list inspection and reordering, used to gradually shrink btree files. */

#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_am.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "dbinc/txn.h"
#include "dbinc/btree.h"
#include "dbinc_auto/btree_ext.h"
#include "dbinc_auto/lock_ext.h"
#include "dbinc_auto/txn_ext.h"

extern int gbl_is_physical_replicant;

int gbl_btree_shrink_debug_abort = 0;		/* test: abort each sort batch */
int gbl_btree_shrink_trim_debug_abort = 0;	/* test: abort each trim */
int gbl_btree_shrink_move_debug_abort = 0;	/* test: abort each page move */
int gbl_btree_shrink_ovrewrite_debug_abort = 0;	/* test: abort each chain rewrite */
int gbl_btree_shrink_ovmove_max_pages = 256;	/* longest chain moved, in pages */
extern int gbl_btree_shrink_ovmap_max_mb;
u_int64_t gbl_btree_shrink_ovstale;	/* owner hints that were wrong */
u_int64_t gbl_btree_shrink_ovplaced;	/* freed chain pages sorted into place */

#define	FS_ISSET(bm, pg)	((bm)[(pg) >> 3] & (1 << ((pg) & 7)))
#define	FS_SET(bm, pg)		((bm)[(pg) >> 3] |= (1 << ((pg) & 7)))

typedef int (*freelist_cb)(void *arg, db_pgno_t pgno, db_pgno_t prev,
    db_pgno_t next);

/* Call cb for each free page; without locks, stops (*incomplete) on a race. */
static int
__db_freelist_walk(DB *dbp, db_pgno_t *last_pgnop, freelist_cb cb, void *arg,
    int *incomplete, u_int8_t **bitmapp)
{
	DB_ENV *dbenv;
	DB_MPOOLFILE *mpf;
	DBMETA *meta;
	PAGE *h;
	db_pgno_t pg, prev, next, last_pgno;
	u_int8_t *bitmap;
	int ret;

	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	*incomplete = 0;

	pg = PGNO_BASE_MD;
	if ((ret = __memp_fget(mpf, &pg, 0, &meta)) != 0)
		return (ret);
	*last_pgnop = last_pgno = meta->last_pgno;
	pg = meta->free;
	if ((ret = __memp_fput(mpf, meta, 0)) != 0)
		return (ret);

	if ((ret = __os_calloc(dbenv, 1, last_pgno / 8 + 1, &bitmap)) != 0)
		return (ret);

	prev = PGNO_BASE_MD;
	while (pg != PGNO_INVALID) {
		if (pg > last_pgno || FS_ISSET(bitmap, pg)) {
			*incomplete = 1;
			break;
		}
		if ((ret = __memp_fget(mpf, &pg, 0, &h)) != 0)
			goto err;
		if (TYPE(h) != P_INVALID) {
			*incomplete = 1;
			ret = __memp_fput(mpf, h, 0);
			break;
		}
		next = NEXT_PGNO(h);
		if ((ret = __memp_fput(mpf, h, 0)) != 0)
			goto err;
		FS_SET(bitmap, pg);
		if ((ret = cb(arg, pg, prev, next)) != 0)
			goto err;
		prev = pg;
		pg = next;
	}

err:	if (ret == 0 && bitmapp != NULL)
		*bitmapp = bitmap;
	else
		__os_free(dbenv, bitmap);
	return (ret);
}

static int
__db_freespace_stat_cb(void *arg, db_pgno_t pgno, db_pgno_t prev,
    db_pgno_t next)
{
	struct __db_freespace_stat *st = arg;

	COMPQUIET(prev, 0);
	st->nfree++;
	if (next != PGNO_INVALID && next > pgno)
		st->nascending++;
	return (0);
}

/* Walk the free list and summarize how much of the file is reclaimable. */
int
__db_freespace_stat(DB *dbp, struct __db_freespace_stat *st)
{
	DB_MPOOLFILE *mpf;
	db_pgno_t pg;
	u_int32_t mbytes, bytes;
	u_int8_t *bitmap;
	int ret;

	mpf = dbp->mpf;
	memset(st, 0, sizeof(*st));
	st->pagesize = dbp->pgsize;

	if (__os_ioinfo(dbp->dbenv, NULL, mpf->fhp, &mbytes, &bytes, NULL) == 0)
		st->file_bytes = (u_int64_t)mbytes * MEGABYTE + bytes;

	if ((ret = __db_freelist_walk(dbp, &st->last_pgno,
	    __db_freespace_stat_cb, st, &st->incomplete, &bitmap)) != 0)
		return (ret);

	for (pg = st->last_pgno; pg > PGNO_BASE_MD && FS_ISSET(bitmap, pg); pg--)
		st->ntail_free++;
	__os_free(dbp->dbenv, bitmap);
	return (0);
}

/* Read the meta page's LSN (which changes with the free list) and last_pgno. */
int
__db_freelist_peek(DB *dbp, DB_LSN *meta_lsnp, db_pgno_t *last_pgnop)
{
	DBMETA *meta;
	db_pgno_t pg;
	int ret;

	pg = PGNO_BASE_MD;
	if ((ret = __memp_fget(dbp->mpf, &pg, 0, &meta)) != 0)
		return (ret);
	*meta_lsnp = LSN(meta);
	*last_pgnop = meta->last_pgno;
	return (__memp_fput(dbp->mpf, meta, 0));
}

/* Log and move free page pgno from after prev to after dest (PGNO_BASE_MD: the head). */
static int
__db_pg_flmove(DBC *dbc, DBMETA *meta, db_pgno_t pgno, db_pgno_t prev,
    db_pgno_t dest, db_pgno_t *old_nextp, db_pgno_t *dest_old_nextp)
{
	DB *dbp;
	DB_MPOOLFILE *mpf;
	DB_LSN lsn;
	PAGE *pp, *xp, *dp;
	__db_pg_flmove_args args;
	u_int32_t putflags;
	int ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	pp = xp = dp = NULL;
	putflags = 0;

	if (!DBC_LOGGING(dbc))
		return (EPERM);
	if (pgno == PGNO_BASE_MD || pgno == prev || pgno == dest ||
	    prev == dest)
		return (EINVAL);
	if (pgno > meta->last_pgno || prev > meta->last_pgno ||
	    dest > meta->last_pgno)
		return (DB_NOTFOUND);

	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &xp)) != 0)
		goto err;
	if (prev != PGNO_BASE_MD &&
	    (ret = PAGEGET(dbc, mpf, &prev, 0, &pp)) != 0)
		goto err;
	if (dest != PGNO_BASE_MD &&
	    (ret = PAGEGET(dbc, mpf, &dest, 0, &dp)) != 0)
		goto err;

	/* DB_NOTFOUND: the list no longer looks the way the caller thinks. */
	if (TYPE(xp) != P_INVALID ||
	    (pp != NULL && (TYPE(pp) != P_INVALID || NEXT_PGNO(pp) != pgno)) ||
	    (pp == NULL && meta->free != pgno) ||
	    (dp != NULL && TYPE(dp) != P_INVALID)) {
		ret = DB_NOTFOUND;
		goto err;
	}

	memset(&args, 0, sizeof(args));
	args.meta_pgno = PGNO_BASE_MD;
	args.meta_lsn = LSN(meta);
	args.pgno = pgno;
	args.pgno_lsn = LSN(xp);
	args.prev_pgno = prev;
	args.prevpg_lsn = pp != NULL ? LSN(pp) : LSN(meta);
	args.dest_pgno = dest;
	args.destpg_lsn = dp != NULL ? LSN(dp) : LSN(meta);
	args.old_next = NEXT_PGNO(xp);
	args.dest_old_next = dp != NULL ? NEXT_PGNO(dp) : meta->free;
	if (args.dest_old_next == pgno || args.old_next > meta->last_pgno ||
	    args.dest_old_next > meta->last_pgno) {
		ret = DB_NOTFOUND;
		goto err;
	}

	if ((ret = __db_pg_flmove_log(dbp, dbc->txn, &lsn, 0,
	    args.meta_pgno, &args.meta_lsn, args.pgno, &args.pgno_lsn,
	    args.prev_pgno, &args.prevpg_lsn, args.dest_pgno, &args.destpg_lsn,
	    args.old_next, args.dest_old_next)) != 0)
		goto err;
	putflags = DB_MPOOL_DIRTY;

	/* Apply exactly what recovery would redo. */
	(void)__db_pg_flmove_apply((PAGE *)meta, PGNO_BASE_MD, &args, &lsn, 1);
	(void)__db_pg_flmove_apply(xp, pgno, &args, &lsn, 1);
	if (pp != NULL)
		(void)__db_pg_flmove_apply(pp, prev, &args, &lsn, 1);
	if (dp != NULL)
		(void)__db_pg_flmove_apply(dp, dest, &args, &lsn, 1);

	*old_nextp = args.old_next;
	*dest_old_nextp = args.dest_old_next;

err:	if (xp != NULL &&
	    (t_ret = PAGEPUT(dbc, mpf, xp, putflags)) != 0 && ret == 0)
		ret = t_ret;
	if (pp != NULL &&
	    (t_ret = PAGEPUT(dbc, mpf, pp, putflags)) != 0 && ret == 0)
		ret = t_ret;
	if (dp != NULL &&
	    (t_ret = PAGEPUT(dbc, mpf, dp, putflags)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/* A plan: the lowest free pages, ascending; ents[start, n) are still free. */
struct __db_flsort {
	DB_ENV *dbenv;
	u_int8_t fileid[DB_FILE_ID_LEN];
	db_pgno_t last_pgno;
	u_int64_t nfree;	/* free pages on the whole list */
	u_int32_t max;
	u_int32_t start;	/* ents before start have left the free list */
	u_int32_t n;
	u_int32_t next;		/* ents[start, next) are in place */
	struct flsort_ent {
		db_pgno_t pgno;
		db_pgno_t prev;	/* predecessor on the list */
		u_int32_t pos;	/* position on the list when walked */
		u_int32_t keep;	/* already in order: never moved */
	} *ents;
};

/* Max-heap on pgno, so the walk keeps the fs->max lowest free pages. */
static void
flsort_sift_down(struct flsort_ent *h, u_int32_t n, u_int32_t i)
{
	struct flsort_ent t;
	u_int32_t c;

	while ((c = 2 * i + 1) < n) {
		if (c + 1 < n && h[c + 1].pgno > h[c].pgno)
			c++;
		if (h[i].pgno >= h[c].pgno)
			break;
		t = h[i];
		h[i] = h[c];
		h[c] = t;
		i = c;
	}
}

static int
__db_flsort_cb(void *arg, db_pgno_t pgno, db_pgno_t prev, db_pgno_t next)
{
	struct __db_flsort *fs = arg;
	struct flsort_ent t;
	u_int32_t i, p;

	COMPQUIET(next, 0);
	t.pgno = pgno;
	t.prev = prev;
	t.pos = (u_int32_t)fs->nfree++;
	t.keep = 0;
	if (fs->n < fs->max) {
		i = fs->n++;
		fs->ents[i] = t;
		for (; i > 0; i = p) {
			p = (i - 1) / 2;
			if (fs->ents[p].pgno >= fs->ents[i].pgno)
				break;
			t = fs->ents[p];
			fs->ents[p] = fs->ents[i];
			fs->ents[i] = t;
		}
	} else if (pgno < fs->ents[0].pgno) {
		fs->ents[0] = t;
		flsort_sift_down(fs->ents, fs->n, 0);
	}
	return (0);
}

static int
flsort_cmp(const void *a, const void *b)
{
	db_pgno_t x = ((const struct flsort_ent *)a)->pgno;
	db_pgno_t y = ((const struct flsort_ent *)b)->pgno;

	return (x < y ? -1 : x > y);
}

static struct flsort_ent *
flsort_find(struct __db_flsort *fs, db_pgno_t pgno)
{
	struct flsort_ent key;

	key.pgno = pgno;
	return (bsearch(&key, fs->ents + fs->start, fs->n - fs->start,
	    sizeof(key), flsort_cmp));
}

/* Keep the longest run already in list order (an LIS), so sorting moves only the rest. */
static int
flsort_mark_keep(struct __db_flsort *fs)
{
	u_int32_t *tails, *parent, i, lo, hi, mid, len;
	int ret;

	/* Only for a whole-list plan: unplanned pages could sit between kept ones. */
	if (fs->n == 0 || fs->n != fs->nfree)
		return (0);
	if ((ret = __os_malloc(fs->dbenv,
	    2 * fs->n * sizeof(u_int32_t), &tails)) != 0)
		return (ret);
	parent = tails + fs->n;
	for (i = 0, len = 0; i < fs->n; i++) {
		for (lo = 0, hi = len; lo < hi;) {
			mid = (lo + hi) / 2;
			if (fs->ents[tails[mid]].pos < fs->ents[i].pos)
				lo = mid + 1;
			else
				hi = mid;
		}
		parent[i] = lo > 0 ? tails[lo - 1] : UINT32_MAX;
		tails[lo] = i;
		if (lo == len)
			len++;
	}
	for (i = tails[len - 1]; i != UINT32_MAX; i = parent[i])
		fs->ents[i].keep = 1;
	__os_free(fs->dbenv, tails);
	return (0);
}

/* Walk dbp's free list and plan how to sort its lowest max pages to the front. */
int
__db_flsort_create(DB *dbp, u_int32_t max, struct __db_flsort **fsp)
{
	DB_ENV *dbenv;
	struct __db_flsort *fs;
	int incomplete, ret;

	dbenv = dbp->dbenv;
	*fsp = NULL;
	if (max == 0)
		return (EINVAL);
	if ((ret = __os_calloc(dbenv, 1, sizeof(*fs), &fs)) != 0)
		return (ret);
	if ((ret = __os_malloc(dbenv, max * sizeof(fs->ents[0]),
	    &fs->ents)) != 0) {
		__os_free(dbenv, fs);
		return (ret);
	}
	fs->dbenv = dbenv;
	memcpy(fs->fileid, dbp->fileid, DB_FILE_ID_LEN);
	fs->max = max;

	if ((ret = __db_freelist_walk(dbp, &fs->last_pgno, __db_flsort_cb, fs,
	    &incomplete, NULL)) != 0 || incomplete) {
		__db_flsort_destroy(fs);
		return (ret != 0 ? ret : DB_NOTFOUND);
	}
	qsort(fs->ents, fs->n, sizeof(fs->ents[0]), flsort_cmp);
	if ((ret = flsort_mark_keep(fs)) != 0) {
		__db_flsort_destroy(fs);
		return (ret);
	}
	*fsp = fs;
	return (0);
}

void
__db_flsort_destroy(struct __db_flsort *fs)
{
	if (fs == NULL)
		return;
	__os_free(fs->dbenv, fs->ents);
	__os_free(fs->dbenv, fs);
}

void
__db_flsort_info(struct __db_flsort *fs, u_int64_t *nfree,
    db_pgno_t *last_pgno, u_int32_t *placed, u_int32_t *planned)
{
	*nfree = fs->nfree;
	*last_pgno = fs->last_pgno;
	*placed = fs->next;
	*planned = fs->n;
}

/* Up to max_moves pg_flmoves in one txn; NOTGRANTED: busy, other errors: stale plan. */
int
__db_flsort_step(DB *dbp, struct __db_flsort *fs, u_int32_t max_moves,
    u_int32_t *movedp, int *donep, DB_LSN *meta_lsnp)
{
	DB_ENV *dbenv;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	DB_TXN *txn;
	DBC *dbc;
	DBMETA *meta;
	struct flsort_ent *e, *t;
	db_pgno_t pgno, dest, old_next, dest_old_next;
	u_int32_t moved;
	int ret, t_ret;

	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	dbc = NULL;
	meta = NULL;
	txn = NULL;
	LOCK_INIT(metalock);
	*movedp = moved = 0;
	*donep = 0;

	if (memcmp(fs->fileid, dbp->fileid, DB_FILE_ID_LEN) != 0)
		return (EINVAL);
	/* Only a (non-physrep) master changes free lists. */
	if (IS_REP_CLIENT(dbenv) || !LOGGING_ON(dbenv) ||
	    gbl_is_physical_replicant)
		return (EPERM);

	/* Never wait on a lock, and be the victim of any deadlock. */
	if ((ret = __txn_begin(dbenv, NULL, &txn, DB_TXN_NOWAIT)) != 0)
		return (ret);
	if ((ret = __lock_locker_set_lowpri(dbenv, txn->txnid)) != 0)
		goto err;
	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		goto err;

	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;

	/* Put each page after its sorted predecessor, ascending. */
	while (moved < max_moves && fs->next < fs->n) {
		e = &fs->ents[fs->next];
		dest = fs->next == fs->start ?
		    PGNO_BASE_MD : fs->ents[fs->next - 1].pgno;
		if (e->keep || e->prev == dest) {
			fs->next++;
			continue;
		}
		if ((ret = __db_pg_flmove(dbc, meta, e->pgno, e->prev, dest,
		    &old_next, &dest_old_next)) != 0)
			break;
		if ((t = flsort_find(fs, old_next)) != NULL)
			t->prev = e->prev;
		if ((t = flsort_find(fs, dest_old_next)) != NULL)
			t->prev = e->pgno;
		e->prev = dest;
		fs->next++;
		moved++;
	}
	/* Pages freed meanwhile go in front of the sorted run: then re-plan. */
	if (ret == 0 && fs->next == fs->n) {
		if (fs->n == fs->start ||
		    meta->free == fs->ents[fs->start].pgno)
			*donep = 1;
		else
			ret = DB_NOTFOUND;
	}
	/* The meta LSN of the state just verified, for the caller's "seen" check. */
	if (meta != NULL)
		*meta_lsnp = LSN(meta);

err:	if (meta != NULL && (t_ret = PAGEPUT(dbc, mpf, meta,
	    moved > 0 ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (ret == DB_LOCK_DEADLOCK)
		ret = DB_LOCK_NOTGRANTED;	/* how __db_lget reports NOWAIT */
	if (ret == 0 && moved > 0 && gbl_btree_shrink_debug_abort) {
		(void)__txn_abort(txn);
		return (DB_NOTFOUND);
	}
	/* Each move is self-consistent, so keep those already made. */
	if (ret == DB_NOTFOUND && moved > 0) {
		if ((t_ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0)
			ret = t_ret;
	} else if (ret == 0) {
		ret = __txn_commit(txn, DB_TXN_NOSYNC);
	} else if (txn != NULL)
		(void)__txn_abort(txn);
	if (ret == 0 || ret == DB_NOTFOUND)
		*movedp = moved;
	return (ret);
}

/* Log and drop free pages first..last_pgno, the list's tail after prev. */
static int
__db_pg_trunc(DBC *dbc, DBMETA *meta, db_pgno_t prev, db_pgno_t first)
{
	DB *dbp;
	DB_LSN lsn;
	DB_MPOOLFILE *mpf;
	PAGE *h, *pp;
	__db_pg_trunc_args args;
	db_pgno_t pg, last;
	int ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	pp = NULL;
	last = meta->last_pgno;

	if (!DBC_LOGGING(dbc))
		return (EPERM);
	if (first <= PGNO_BASE_MD + 1 || first > last ||
	    (prev != PGNO_BASE_MD && prev >= first))
		return (EINVAL);

	if (prev == PGNO_BASE_MD) {
		if (meta->free != first)
			return (DB_NOTFOUND);
	} else {
		if ((ret = PAGEGET(dbc, mpf, &prev, 0, &pp)) != 0)
			return (ret);
		if (TYPE(pp) != P_INVALID || NEXT_PGNO(pp) != first) {
			ret = DB_NOTFOUND;
			goto err;
		}
	}

	/* The run must be exactly first, first+1, ..., last, then the end. */
	for (pg = first; pg <= last; pg++) {
		if ((ret = PAGEGET(dbc, mpf, &pg, 0, &h)) != 0)
			goto err;
		if (TYPE(h) != P_INVALID ||
		    NEXT_PGNO(h) != (pg == last ? PGNO_INVALID : pg + 1))
			ret = DB_NOTFOUND;
		if ((t_ret = PAGEPUT(dbc, mpf, h, 0)) != 0 && ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto err;
	}

	memset(&args, 0, sizeof(args));
	args.meta_pgno = PGNO_BASE_MD;
	args.meta_lsn = LSN(meta);
	args.prev_pgno = prev;
	args.prevpg_lsn = pp != NULL ? LSN(pp) : LSN(meta);
	args.first_pgno = first;
	args.old_last = last;
	args.new_last = first - 1;
	args.old_metaflags = meta->metaflags;

	if ((ret = __db_pg_trunc_log(dbp, dbc->txn, &lsn, 0,
	    args.meta_pgno, &args.meta_lsn, args.prev_pgno, &args.prevpg_lsn,
	    args.first_pgno, args.old_last, args.new_last,
	    args.old_metaflags)) != 0)
		goto err;

	/* Apply exactly what recovery would redo; the file is cut only once this log is gone. */
	(void)__db_pg_trunc_apply((PAGE *)meta, PGNO_BASE_MD, &args, &lsn, 1);
	if (pp != NULL)
		(void)__db_pg_trunc_apply(pp, prev, &args, &lsn, 1);
	__memp_set_trunc_wait(mpf, lsn.file);

	if (pp != NULL) {
		ret = PAGEPUT(dbc, mpf, pp, DB_MPOOL_DIRTY);
		pp = NULL;
	}
err:	if (pp != NULL && (t_ret = PAGEPUT(dbc, mpf, pp, 0)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

/* Trim up to max_pages of a sorted list's free run at the end of the file, in one txn. */
int
__db_fltrim_step(DB *dbp, struct __db_flsort *fs, u_int32_t max_pages,
    u_int32_t *trimmedp, int *donep, DB_LSN *meta_lsnp)
{
	DB_ENV *dbenv;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	DB_TXN *txn;
	DBC *dbc;
	DBMETA *meta;
	db_pgno_t pgno, first, prev;
	u_int32_t run, k, i0;
	int ret, t_ret;

	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	dbc = NULL;
	meta = NULL;
	txn = NULL;
	LOCK_INIT(metalock);
	*trimmedp = 0;
	*donep = 0;

	if (memcmp(fs->fileid, dbp->fileid, DB_FILE_ID_LEN) != 0)
		return (EINVAL);
	if (IS_REP_CLIENT(dbenv) || !LOGGING_ON(dbenv) ||
	    gbl_is_physical_replicant)
		return (EPERM);

	/* Only a plan of the whole, sorted list knows the tail. */
	for (run = 0; run < fs->n - fs->start &&
	    fs->ents[fs->n - 1 - run].pgno == fs->last_pgno - run; run++)
		;
	if (fs->n - fs->start != fs->nfree || fs->next != fs->n || run == 0 ||
	    max_pages == 0) {
		*donep = 1;
		return (__db_freelist_peek(dbp, meta_lsnp, &pgno));
	}
	k = run < max_pages ? run : max_pages;
	i0 = fs->n - k;
	first = fs->ents[i0].pgno;
	prev = i0 == fs->start ? PGNO_BASE_MD : fs->ents[i0 - 1].pgno;

	if ((ret = __txn_begin(dbenv, NULL, &txn, DB_TXN_NOWAIT)) != 0)
		return (ret);
	if ((ret = __lock_locker_set_lowpri(dbenv, txn->txnid)) != 0)
		goto err;
	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		goto err;
	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;
	if (meta->last_pgno != fs->last_pgno)
		ret = DB_NOTFOUND;
	else
		ret = __db_pg_trunc(dbc, meta, prev, first);
	*meta_lsnp = LSN(meta);

err:	if (meta != NULL && (t_ret = PAGEPUT(dbc, mpf, meta,
	    ret == 0 ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (ret == DB_LOCK_DEADLOCK)
		ret = DB_LOCK_NOTGRANTED;
	if (ret == 0 && gbl_btree_shrink_trim_debug_abort)
		ret = DB_NOTFOUND;
	if (ret != 0) {
		(void)__txn_abort(txn);
		return (ret);
	}
	/* A failed commit aborts the transaction itself. */
	if ((ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0)
		return (ret);
	fs->n = i0;
	fs->next = i0;
	fs->nfree -= k;
	fs->last_pgno = first - 1;
	*trimmedp = k;
	*donep = (k == run);
	return (0);
}

/* Cut trimmed pages once first_logfile is past the trim's log; else set *waitp. */
int
__db_physical_truncate(DB *dbp, u_int32_t first_logfile,
    u_int32_t cur_logfile, u_int32_t *pagesp, u_int32_t *waitp)
{
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	DBC *dbc;
	DBMETA *meta;
	db_pgno_t pgno, mlast;
	u_int32_t wait;
	int ret, t_ret;

	mpf = dbp->mpf;
	meta = NULL;
	LOCK_INIT(metalock);
	*pagesp = 0;
	*waitp = 0;

	/* Peek without the lock: most btrees have nothing to cut. */
	pgno = PGNO_BASE_MD;
	if ((ret = __memp_fget(mpf, &pgno, 0, &meta)) != 0)
		return (ret);
	__memp_last_pgno(mpf, &mlast);
	t_ret = FLD_ISSET(meta->metaflags, DBMETA_TRIMMED) &&
	    meta->last_pgno < mlast;
	if ((ret = __memp_fput(mpf, meta, 0)) != 0)
		return (ret);
	meta = NULL;
	if (!t_ret)
		return (0);

	if ((ret = __db_cursor(dbp, NULL, &dbc, 0)) != 0)
		return (ret);
	/* The meta lock keeps allocations (and rep apply) out while we cut. */
	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc, LCK_ALWAYS, pgno, DB_LOCK_WRITE,
	    DB_LOCK_NOWAIT, &metalock)) != 0) {
		if (ret == DB_LOCK_DEADLOCK)
			ret = DB_LOCK_NOTGRANTED;
		goto err;
	}
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;

	__memp_last_pgno(mpf, &mlast);
	if (!FLD_ISSET(meta->metaflags, DBMETA_TRIMMED) ||
	    meta->last_pgno >= mlast)
		goto err;

	/* Trimmed before a restart: the log tag is lost, so wait from now. */
	if ((wait = __memp_get_trunc_wait(mpf)) == 0) {
		__memp_set_trunc_wait(mpf, cur_logfile);
		wait = cur_logfile;
	}
	if (first_logfile <= wait)
		*waitp = wait;
	else if ((ret = __memp_ftruncate(mpf, meta->last_pgno)) == 0)
		*pagesp = mlast - meta->last_pgno;

err:	if (meta != NULL && (t_ret = PAGEPUT(dbc, mpf, meta, 0)) != 0 &&
	    ret == 0)
		ret = t_ret;
	(void)__TLPUT(dbc, metalock);
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	return (ret);
}

static int __db_ovmove_step __P((DB *, struct __db_flsort *,
    struct __db_ovmap *, db_pgno_t, int, u_int32_t *, u_int32_t *, int *,
    int *));

/* Move the live page at the end of the file into the lowest free page, then trim it. */
int
__db_btmove_step(DB *dbp, struct __db_flsort *fs, int allow_overflow,
    struct __db_ovmap **ovmapp, u_int32_t *movedp, u_int32_t *trimmedp,
    int *stopp, int *replanp)
{
	DB_ENV *dbenv;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	DB_TXN *txn;
	DBC *dbc;
	DBMETA *meta;
	PAGE *tp;
	db_pgno_t pgno, H, L, T, old_next, dest_old_next;
	int have_tail, ret, t_ret;

	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	dbc = NULL;
	meta = NULL;
	txn = NULL;
	LOCK_INIT(metalock);
	*movedp = 0;
	*trimmedp = 0;
	*stopp = 0;
	*replanp = 0;

	if (memcmp(fs->fileid, dbp->fileid, DB_FILE_ID_LEN) != 0)
		return (EINVAL);
	if (IS_REP_CLIENT(dbenv) || !LOGGING_ON(dbenv) ||
	    gbl_is_physical_replicant)
		return (EPERM);
	/* The plan must know the whole, sorted list, with its tail trimmed. */
	if (fs->next != fs->n)
		return (DB_NOTFOUND);
	if (fs->n - fs->start != fs->nfree || fs->n == fs->start ||
	    fs->ents[fs->start].pgno >= fs->last_pgno) {
		*stopp = DB_BTMOVE_STOP_NOFREE;
		return (0);
	}
	if (fs->ents[fs->n - 1].pgno == fs->last_pgno)
		return (0);	/* a free tail: trim first */

	/* An overflow page moves by rewriting its chain's owner. */
	pgno = fs->last_pgno;
	if ((ret = __memp_fget(mpf, &pgno, 0, &tp)) != 0)
		return (ret);
	have_tail = TYPE(tp) == P_OVERFLOW;
	(void)__memp_fput(mpf, tp, 0);
	if (have_tail) {
		if (allow_overflow == DB_BTMOVE_OV_NO) {
			*stopp = DB_BTMOVE_STOP_PAGE;
			return (0);
		}
		if (allow_overflow == DB_BTMOVE_OV_LATER)
			return (EAGAIN);
		if (*ovmapp != NULL && (gbl_btree_shrink_ovmap_max_mb <= 0 ||
		    !__db_ovmap_matches(*ovmapp, dbp))) {
			__db_ovmap_destroy(*ovmapp);
			*ovmapp = NULL;
		}
		if (*ovmapp == NULL && gbl_btree_shrink_ovmap_max_mb > 0 &&
		    (ret = __db_ovmap_create(dbp, ovmapp)) != 0)
			return (ret);
		return (__db_ovmove_step(dbp, fs, *ovmapp, fs->last_pgno,
		    allow_overflow == DB_BTMOVE_OV_NOSNAP,
		    movedp, trimmedp, stopp, replanp));
	}

	L = fs->ents[fs->start].pgno;
	have_tail = fs->start + 1 < fs->n;
	T = have_tail ? fs->ents[fs->n - 1].pgno : PGNO_INVALID;

	if ((ret = __txn_begin(dbenv, NULL, &txn, DB_TXN_NOWAIT)) != 0)
		return (ret);
	if ((ret = __lock_locker_set_lowpri(dbenv, txn->txnid)) != 0)
		goto err;
	if ((ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
		goto err;
	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0)
		goto err;
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;
	H = meta->last_pgno;
	if (H != fs->last_pgno) {
		ret = DB_NOTFOUND;
		goto err;
	}
	/* T must still be the last free page. */
	if (have_tail) {
		pgno = T;
		if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &tp)) != 0)
			goto err;
		if (TYPE(tp) != P_INVALID || NEXT_PGNO(tp) != PGNO_INVALID)
			ret = DB_NOTFOUND;
		if ((t_ret = PAGEPUT(dbc, mpf, tp, 0)) != 0 && ret == 0)
			ret = t_ret;
		if (ret != 0)
			goto err;
	}

	if ((ret = __bam_pgmove(dbc, meta, H, L)) != 0)
		goto err;
	/* H is now the free-list head: send it to the end and trim it off. */
	if (have_tail) {
		if ((ret = __db_pg_flmove(dbc, meta, H, PGNO_BASE_MD, T,
		    &old_next, &dest_old_next)) != 0 ||
		    (ret = __db_pg_trunc(dbc, meta, T, H)) != 0)
			goto err;
	} else if ((ret = __db_pg_trunc(dbc, meta, PGNO_BASE_MD, H)) != 0)
		goto err;

err:	if (meta != NULL && (t_ret = PAGEPUT(dbc, mpf, meta,
	    ret == 0 ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (ret == 0 && gbl_btree_shrink_move_debug_abort)
		ret = DB_NOTFOUND;
	if (ret == DB_BTMOVE_STOP) {
		*stopp = DB_BTMOVE_STOP_PAGE;
		(void)__txn_abort(txn);
		return (0);
	}
	if (ret != 0) {
		if (txn != NULL)
			(void)__txn_abort(txn);
		return (ret == DB_LOCK_DEADLOCK ? DB_LOCK_NOTGRANTED : ret);
	}
	/* A failed commit aborts the transaction itself. */
	if ((ret = __txn_commit(txn, DB_TXN_NOSYNC)) != 0)
		return (ret);
	fs->start++;
	fs->nfree--;
	fs->last_pgno = H - 1;
	*movedp = 1;
	return (0);
}

static int
pgno_cmp(const void *a, const void *b)
{
	db_pgno_t x = *(const db_pgno_t *)a, y = *(const db_pgno_t *)b;

	return (x < y ? -1 : x > y);
}

/* In the rewrite's txn: trim the freed chain's pages ending at H; sort in the rest. */
static int
__db_ovtrim(DBC *dbc, struct __db_flsort *fs, db_pgno_t *old, u_int32_t n,
    db_pgno_t H, u_int32_t *trimmedp, db_pgno_t **listp, u_int32_t *nlistp,
    u_int32_t *placedp)
{
	DB *dbp;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	DBMETA *meta;
	PAGE *tp;
	db_pgno_t *mdl, *lst, *lv, first, p, pgno, prev, dest, placed, on, don;
	u_int32_t cnt, i, j, k, len, nlv, nrs, lo, hi, mid;
	int rest, ret, t_ret;

	dbp = dbc->dbp;
	mpf = dbp->mpf;
	meta = NULL;
	mdl = lst = NULL;
	LOCK_INIT(metalock);
	*trimmedp = 0;
	*listp = NULL;
	*nlistp = 0;
	*placedp = 0;

	/* The list is now old[n-1], ..., old[0], then the plan's unused pages. */
	for (k = 0;; k++) {
		for (i = 0; i < n && old[i] != H - k; i++)
			;
		if (i == n)
			break;
	}
	if (k == 0)
		return (0);
	rest = fs->start + n < fs->n;
	if ((ret = __os_malloc(dbp->dbenv,
	    (n + 1) * sizeof(*mdl), &mdl)) != 0)
		return (ret);
	for (cnt = 0; cnt < n; cnt++)
		mdl[cnt] = old[n - 1 - cnt];
	if (rest)
		mdl[cnt++] = fs->ents[fs->n - 1].pgno;

	pgno = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pgno, DB_LOCK_WRITE, 0, &metalock)) != 0 ||
	    (ret = PAGEGET(dbc, mpf, &pgno, 0, &meta)) != 0)
		goto err;
	/* Any mismatch below leaves the list to a re-plan. */
	if (meta->last_pgno != H || meta->free != mdl[0])
		goto err;
	if (rest) {
		pgno = mdl[cnt - 1];
		if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &tp)) != 0)
			goto err;
		rest = TYPE(tp) == P_INVALID && NEXT_PGNO(tp) == PGNO_INVALID;
		if ((ret = PAGEPUT(dbc, mpf, tp, 0)) != 0 || !rest)
			goto err;
	}

	/* Pages H-k+1..H, ascending, each to the end of the list; then trim them. */
	first = H - k + 1;
	for (p = first; p <= H; p++) {
		for (i = 0; mdl[i] != p; i++)
			;
		if (i == cnt - 1)
			continue;
		prev = i == 0 ? PGNO_BASE_MD : mdl[i - 1];
		if ((ret = __db_pg_flmove(dbc, meta, p, prev, mdl[cnt - 1],
		    &on, &don)) != 0)
			goto err;
		for (j = i; j < cnt - 1; j++)
			mdl[j] = mdl[j + 1];
		mdl[cnt - 1] = p;
	}
	for (i = 0; mdl[i] != first; i++)
		;
	if ((ret = __db_pg_trunc(dbc, meta,
	    i == 0 ? PGNO_BASE_MD : mdl[i - 1], first)) != 0)
		goto err;
	*trimmedp = k;

	/* Now: the chain's other pages (lv) then the unused plan pages; insert lv ascending. */
	nrs = fs->n - (fs->start + n);
	if ((ret = __os_malloc(dbp->dbenv,
	    (2 * n + nrs) * sizeof(*lst), &lst)) != 0)
		goto err;
	lv = lst + n + nrs;
	for (i = 0, len = 0, nlv = 0; i < n; i++) {
		if ((p = old[n - 1 - i]) >= first)
			continue;
		lst[len++] = p;
		lv[nlv++] = p;
	}
	for (i = 0; i < nrs; i++)
		lst[len++] = fs->ents[fs->start + n + i].pgno;
	qsort(lv, nlv, sizeof(*lv), pgno_cmp);
	for (j = 0, placed = PGNO_INVALID; j < nlv; j++) {
		p = lv[j];
		/* Its sorted predecessor: the highest lower unused plan page or placed page. */
		for (lo = 0, hi = nrs; lo < hi;) {
			mid = (lo + hi) / 2;
			if (fs->ents[fs->start + n + mid].pgno < p)
				lo = mid + 1;
			else
				hi = mid;
		}
		dest = lo > 0 ? fs->ents[fs->start + n + lo - 1].pgno : PGNO_BASE_MD;
		if (placed != PGNO_INVALID && placed > dest)
			dest = placed;
		for (i = 0; lst[i] != p; i++)
			;
		prev = i == 0 ? PGNO_BASE_MD : lst[i - 1];
		placed = p;
		if (prev == dest)
			continue;
		if ((ret = __db_pg_flmove(dbc, meta, p, prev, dest,
		    &on, &don)) != 0)
			goto err;
		++*placedp;
		memmove(&lst[i], &lst[i + 1], (len - i - 1) * sizeof(*lst));
		if (dest == PGNO_BASE_MD)
			i = 0;
		else {
			for (i = 0; lst[i] != dest; i++)
				;
			i++;
		}
		memmove(&lst[i + 1], &lst[i], (len - 1 - i) * sizeof(*lst));
		lst[i] = p;
	}
	/* Checks our model only; a stale plan is caught later by page checks. */
	for (i = 1; i < len; i++)
		if (lst[i - 1] >= lst[i])
			goto err;
	*listp = lst;
	*nlistp = len;
	lst = NULL;

	/* A mismatch keeps the rewrite: each move made so far is self-consistent. */
err:	if (ret == DB_NOTFOUND)
		ret = 0;
	if (meta != NULL && (t_ret = PAGEPUT(dbc, mpf, meta,
	    DB_MPOOL_DIRTY)) != 0 && ret == 0)
		ret = t_ret;
	(void)__TLPUT(dbc, metalock);
	__os_free(dbp->dbenv, mdl);
	if (lst != NULL)
		__os_free(dbp->dbenv, lst);
	return (ret);
}

/* After a committed rewrite, the plan's whole list is lst, sorted. */
static void
__db_flsort_patch(struct __db_flsort *fs, db_pgno_t *lst, u_int32_t len,
    db_pgno_t last_pgno)
{
	u_int32_t i;

	for (i = 0; i < len; i++) {
		fs->ents[i].pgno = lst[i];
		fs->ents[i].prev = i == 0 ? PGNO_BASE_MD : lst[i - 1];
		fs->ents[i].pos = i;
		fs->ents[i].keep = 1;
	}
	fs->start = 0;
	fs->n = fs->next = len;
	fs->nfree = len;
	fs->last_pgno = last_pgno;
}

/* Last page H is in an overflow chain: rewrite its owner into the lowest free pages. */
static int
__db_ovmove_step(DB *dbp, struct __db_flsort *fs, struct __db_ovmap *map,
    db_pgno_t H, int nosnap, u_int32_t *movedp, u_int32_t *trimmedp,
    int *stopp, int *replanp)
{
	DB_ENV *dbenv;
	DB_MPOOLFILE *mpf;
	DB_TXN *txn;
	DBC *dbc;
	DBT key;
	PAGE *p;
	db_pgno_t pg, prev, newhead, *want, *old, *lst;
	u_int32_t i, n, nwant, npages, ntrim, nlst, nplaced, cap;
	int attempt, isovfl, keychain, scanned, ret, t_ret;

	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	dbc = NULL;
	txn = NULL;
	want = old = lst = NULL;
	npages = ntrim = nlst = nplaced = 0;
	newhead = PGNO_INVALID;
	memset(&key, 0, sizeof(key));
	cap = (u_int32_t)gbl_btree_shrink_ovmove_max_pages;

	/* Unlocked walk back to the chain's head; a race just means skip. */
	for (pg = H, n = 0;; n++) {
		if (n >= cap) {
			*stopp = DB_BTMOVE_STOP_OVLONG;
			return (0);
		}
		if ((ret = __memp_fget(mpf, &pg, 0, &p)) != 0)
			return (ret);
		isovfl = TYPE(p) == P_OVERFLOW;
		prev = PREV_PGNO(p);
		(void)__memp_fput(mpf, p, 0);
		if (!isovfl)
			return (DB_NOTFOUND);
		if (prev == PGNO_INVALID)
			break;
		pg = prev;
	}

	nwant = fs->n - fs->start;
	if (nwant > cap)
		nwant = cap;
	if ((ret = __os_malloc(dbenv, nwant * sizeof(*want), &want)) != 0 ||
	    (ret = __os_malloc(dbenv, cap * sizeof(*old), &old)) != 0)
		goto err;
	for (i = 0; i < nwant; i++)
		want[i] = fs->ents[fs->start + i].pgno;

	/* A wrong owner hint gets one retry, through a fresh scan. */
	for (attempt = 0;; attempt++) {
		/* No leaf owner: a key chain kept by an internal page, or leaked. */
		if ((ret = __bam_ovowner(dbp, map, pg, &key,
		    &keychain, &scanned)) == DB_NOTFOUND)
			keychain = 1;
		else if (ret != 0)
			goto err;
		if (keychain) {
			*stopp = DB_BTMOVE_STOP_OVKEY;
			goto err;
		}

		if ((ret = __txn_begin(dbenv, NULL, &txn, DB_TXN_NOWAIT)) != 0)
			goto err;
		if ((ret = __lock_locker_set_lowpri(dbenv, txn->txnid)) != 0 ||
		    (ret = __db_cursor(dbp, txn, &dbc, 0)) != 0)
			goto err;
		ret = __bam_ovrewrite(dbc, &key, H, want, nwant, cap,
		    old, &npages, &newhead, stopp);
		/* A hinted owner may be wrong, so its not-found or stop gets one rescan. */
		if ((ret != DB_NOTFOUND && ret != DB_BTMOVE_STOP) || scanned ||
		    attempt > 0)
			break;
		++gbl_btree_shrink_ovstale;
		__bam_ovmap_moved(map, pg, PGNO_INVALID);
		(void)__db_c_close(dbc);
		dbc = NULL;
		(void)__txn_abort(txn);
		txn = NULL;
	}
	if (ret == DB_BTMOVE_PLAN)
		ret = DB_NOTFOUND;
	if (ret == 0)
		ret = __db_ovtrim(dbc, fs, old, npages, H, &ntrim, &lst, &nlst,
		    &nplaced);

err:	if (dbc != NULL && (t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (ret == 0 && txn != NULL && gbl_btree_shrink_ovrewrite_debug_abort)
		ret = DB_NOTFOUND;
	/* Re-check just before commit: a new snapshot reader could read the old chain. */
	if (ret == 0 && txn != NULL && nosnap) {
		Pthread_mutex_lock(&dbenv->outstanding_modsnap_lock);
		if (listc_size(&dbenv->outstanding_modsnaps) != 0)
			ret = EAGAIN;
		Pthread_mutex_unlock(&dbenv->outstanding_modsnap_lock);
	}
	if (ret == DB_BTMOVE_STOP)
		ret = 0;	/* *stopp says why; nothing was changed */
	else if (ret == 0 && txn != NULL) {
		if ((ret = __txn_commit(txn, DB_TXN_NOSYNC)) == 0) {
			__bam_ovmap_moved(map, pg, newhead);
			gbl_btree_shrink_ovplaced += nplaced;
			*movedp = npages;
			*trimmedp = ntrim;
			if (lst != NULL) {
				__db_flsort_patch(fs, lst, nlst, H - ntrim);
				*replanp = DB_BTMOVE_PATCHED;
			} else
				*replanp = DB_BTMOVE_REPLAN;
		}
		txn = NULL;
	}
	if (txn != NULL)
		(void)__txn_abort(txn);
	if (key.data != NULL)
		__os_free(dbenv, key.data);
	if (want != NULL)
		__os_free(dbenv, want);
	if (old != NULL)
		__os_free(dbenv, old);
	if (lst != NULL)
		__os_free(dbenv, lst);
	return (ret == DB_LOCK_DEADLOCK ? DB_LOCK_NOTGRANTED : ret);
}
