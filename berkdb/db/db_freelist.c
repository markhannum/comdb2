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
