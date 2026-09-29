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

/*
 * Physically truncating a file which btree_shrink trimmed.
 */

#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/db_swap.h"
#include "dbinc/mp.h"
#include "comdb2_atomic.h"

/*
 * __memp_set_trunc_wait -- trimmed pages can't be cut until log 'file' is deleted.
 * PUBLIC: void __memp_set_trunc_wait __P((DB_MPOOLFILE *, u_int32_t));
 */
void
__memp_set_trunc_wait(dbmfp, file)
	DB_MPOOLFILE *dbmfp;
	u_int32_t file;
{
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;

	dbmp = dbmfp->dbenv->mp_handle;
	mfp = dbmfp->mfp;
	R_LOCK(dbmfp->dbenv, dbmp->reginfo);
	if (file > mfp->trunc_wait_file)
		mfp->trunc_wait_file = file;
	R_UNLOCK(dbmfp->dbenv, dbmp->reginfo);
}

/*
 * __memp_get_trunc_wait --
 *
 * PUBLIC: u_int32_t __memp_get_trunc_wait __P((DB_MPOOLFILE *));
 */
u_int32_t
__memp_get_trunc_wait(dbmfp)
	DB_MPOOLFILE *dbmfp;
{
	DB_MPOOL *dbmp;
	u_int32_t file;

	dbmp = dbmfp->dbenv->mp_handle;
	R_LOCK(dbmfp->dbenv, dbmp->reginfo);
	file = dbmfp->mfp->trunc_wait_file;
	R_UNLOCK(dbmfp->dbenv, dbmp->reginfo);
	return (file);
}

/* Drop this bucket's buffers of pages lo < pgno <= hi; *busyp if one is in use. */
static void
__memp_discard_bucket(dbmp, c_mp, hp, mfp, lo, hi, busyp)
	DB_MPOOL *dbmp;
	MPOOL *c_mp;
	DB_MPOOL_HASH *hp;
	MPOOLFILE *mfp;
	db_pgno_t lo, hi;
	int *busyp;
{
	DB_ENV *dbenv;
	BH *bhp;

	dbenv = dbmp->dbenv;
again:	MUTEX_LOCK(dbenv, &hp->hash_mutex);
	for (bhp = SH_TAILQ_FIRST(&hp->hash_bucket, __bh);
	    bhp != NULL; bhp = SH_TAILQ_NEXT(bhp, hq, __bh)) {
		if (bhp->mpf != mfp || bhp->pgno <= lo || bhp->pgno > hi)
			continue;
		if (bhp->ref != 0 || F_ISSET(bhp, BH_LOCKED)) {
			*busyp = 1;
			continue;
		}
		if (F_ISSET(bhp, BH_DIRTY)) {
			ATOMIC_ADD32(hp->hash_page_dirty, -1);
			ATOMIC_ADD32(c_mp->stat.st_page_dirty, -1);
		}
		F_CLR(bhp, BH_DIRTY | BH_DIRTY_CREATE);
		/* Releases the bucket mutex. */
		__memp_bhfree(dbmp, hp, bhp, 1);
		goto again;
	}
	MUTEX_UNLOCK(dbenv, &hp->hash_mutex);
}

static void
__memp_discard_range(dbmfp, lo, hi, busyp)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t lo, hi;
	int *busyp;
{
	DB_MPOOL *dbmp;
	DB_MPOOL_HASH *htab;
	MPOOL *mp, *c_mp;
	MPOOLFILE *mfp;
	db_pgno_t pg;
	u_int32_t i;
	int b;

	dbmp = dbmfp->dbenv->mp_handle;
	mp = dbmp->reginfo[0].primary;
	mfp = dbmfp->mfp;

	/* Look up each page, or scan every bucket if that's cheaper. */
	if ((u_int64_t)(hi - lo) <=
	    (u_int64_t)mp->nreg * ((MPOOL *)dbmp->reginfo[0].primary)->htab_buckets) {
		for (pg = lo + 1; pg <= hi && pg > lo; pg++) {
			c_mp = dbmp->reginfo[NCACHE(mp, mfp, pg)].primary;
			htab = R_ADDR(&dbmp->reginfo[NCACHE(mp, mfp, pg)],
			    c_mp->htab);
			__memp_discard_bucket(dbmp, c_mp,
			    &htab[NBUCKET(c_mp, mfp, pg)], mfp, pg - 1, pg, busyp);
		}
		return;
	}
	for (i = 0; i < mp->nreg; i++) {
		c_mp = dbmp->reginfo[i].primary;
		htab = R_ADDR(&dbmp->reginfo[i], c_mp->htab);
		for (b = 0; b < c_mp->htab_buckets; b++)
			__memp_discard_bucket(dbmp, c_mp,
			    &htab[b], mfp, lo, hi, busyp);
	}
}

/* Point recovery-page slots past new_last at the meta page, so they can't come back. */
static int
__memp_recp_invalidate(dbmfp, new_last)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t new_last;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	DB_PGINFO *pginfo;
	MPOOLFILE *mfp;
	PAGE *slot, *meta;
	size_t nr, nw, pagesize;
	db_pgno_t pg;
	int i, ret;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;
	mfp = dbmfp->mfp;
	pagesize = mfp->stat.st_pagesize;
	if (dbenv->mp_recovery_pages <= 0)
		return (0);
	if (dbmfp->recp == NULL)
		return (EAGAIN);
	pginfo = mfp->pgcookie_len > 0 ?
	    (DB_PGINFO *)R_ADDR(dbmp->reginfo, mfp->pgcookie_off) : NULL;

	if ((ret = __os_malloc(dbenv, 2 * pagesize, &meta)) != 0)
		return (ret);
	slot = (PAGE *)((u_int8_t *)meta + pagesize);

	Pthread_mutex_lock(&dbmfp->recp_lk_array[0]);
	ret = __os_io(dbenv, DB_IO_READ, dbmfp->recp, 0, pagesize,
	    (u_int8_t *)meta, &nr);
	Pthread_mutex_unlock(&dbmfp->recp_lk_array[0]);
	if (ret == 0 && nr < pagesize)
		ret = EAGAIN;

	for (i = 1; ret == 0 && i <= dbenv->mp_recovery_pages; i++) {
		Pthread_mutex_lock(&dbmfp->recp_lk_array[i]);
		if ((ret = __os_io(dbenv, DB_IO_READ, dbmfp->recp, i, pagesize,
		    (u_int8_t *)slot, &nr)) == 0 && nr == pagesize) {
			if (pginfo != NULL && F_ISSET(pginfo, DB_AM_SWAP))
				P_32_COPYSWAP(&PGNO(slot), &pg);
			else
				pg = PGNO(slot);
			if (pg > new_last)
				ret = __os_io(dbenv, DB_IO_WRITE, dbmfp->recp,
				    i, pagesize, (u_int8_t *)meta, &nw);
		}
		Pthread_mutex_unlock(&dbmfp->recp_lk_array[i]);
		/* Replay stops at the first short slot. */
		if (ret == 0 && nr < pagesize)
			break;
	}
	__os_free(dbenv, meta);
	return (ret);
}

/*
 * __memp_ftruncate -- cut the file back to new_last; the caller holds the meta lock.
 * PUBLIC: int __memp_ftruncate __P((DB_MPOOLFILE *, db_pgno_t));
 */
int
__memp_ftruncate(dbmfp, new_last)
	DB_MPOOLFILE *dbmfp;
	db_pgno_t new_last;
{
	DB_ENV *dbenv;
	DB_MPOOL *dbmp;
	MPOOLFILE *mfp;
	db_pgno_t old_last;
	int busy, ret;

	dbenv = dbmfp->dbenv;
	dbmp = dbenv->mp_handle;
	mfp = dbmfp->mfp;
	busy = 0;

	/* Lower last_pgno first so no new buffers appear above it. */
	R_LOCK(dbenv, dbmp->reginfo);
	old_last = mfp->last_pgno;
	if (new_last >= old_last) {
		R_UNLOCK(dbenv, dbmp->reginfo);
		return (0);
	}
	mfp->last_pgno = new_last;
	R_UNLOCK(dbenv, dbmp->reginfo);

	__memp_discard_range(dbmfp, new_last, old_last, &busy);
	if (busy) {
		R_LOCK(dbenv, dbmp->reginfo);
		if (mfp->last_pgno == new_last)
			mfp->last_pgno = old_last;
		R_UNLOCK(dbenv, dbmp->reginfo);
		return (EBUSY);
	}

	if ((ret = __memp_recp_invalidate(dbmfp, new_last)) != 0)
		goto err;
	if (__os_truncate(dbenv, dbmfp->fhp,
	    (off_t)(new_last + 1) * mfp->stat.st_pagesize) != 0) {
		ret = EIO;
		goto err;
	}
	/* The file is cut: keep new_last even if the sync fails. */
	(void)__os_fsync(dbenv, dbmfp->fhp);

	/* Catch any reader which slipped a buffer in before last_pgno fell. */
	__memp_discard_range(dbmfp, new_last, old_last, &busy);

	R_LOCK(dbenv, dbmp->reginfo);
	if (mfp->orig_last_pgno > new_last)
		mfp->orig_last_pgno = new_last;
	if (mfp->alloc_pgno > new_last)
		mfp->alloc_pgno = new_last;
	R_UNLOCK(dbenv, dbmp->reginfo);
	return (0);

err:	R_LOCK(dbenv, dbmp->reginfo);
	if (mfp->last_pgno == new_last)
		mfp->last_pgno = old_last;
	R_UNLOCK(dbenv, dbmp->reginfo);
	return (ret);
}
