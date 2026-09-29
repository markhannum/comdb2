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

/* Moving live btree pages into lower free pages, for btree_shrink. */

#include "db_config.h"

#ifndef NO_SYSTEM_INCLUDES
#include <string.h>
#endif

#include "db_int.h"
#include "dbinc/db_page.h"
#include "dbinc/db_shash.h"
#include "dbinc/btree.h"
#include "dbinc/lock.h"
#include "dbinc/log.h"
#include "dbinc/mp.h"
#include "btree/bt_cache.h"
#include <logmsg.h>

/*
 * __bam_pgmove -- copy live page H into free page L (the list head), then free H.
 * PUBLIC: int __bam_pgmove __P((DBC *, DBMETA *, db_pgno_t, db_pgno_t));
 */
int
__bam_pgmove(DBC *dbc, DBMETA *meta, db_pgno_t H, db_pgno_t L)
{
	BTREE *t;
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_ENV *dbenv;
	DB_LOCK lk, plk, nlk;
	DB_LSN lsn, dstlsn;
	DB_MPOOLFILE *mpf;
	DBT key, hdr, data;
	EPG *epg;
	BINTERNAL *bi;
	PAGE *h, *pp, *np, *lp;
	__bam_pgmove_args args;
	db_pgno_t pgno, prev, next;
	u_int32_t ptype, level;
	int exact, logged, ret, t_ret;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	t = dbp->bt_internal;
	cp = (BTREE_CURSOR *)dbc->internal;
	h = pp = np = lp = NULL;
	logged = 0;
	LOCK_INIT(lk);
	LOCK_INIT(plk);
	LOCK_INIT(nlk);
	memset(&key, 0, sizeof(key));
	/* A recycled cursor's stack can hold a stale page: err releases it. */
	BT_STK_CLR(cp);

	if (!DBC_LOGGING(dbc))
		return (EPERM);
	if (H == dbc->internal->root || F_ISSET(dbp, DB_AM_RECNUM))
		return (DB_BTMOVE_STOP);

	/* Peek at H for its first key; drop the lock right away. */
	pgno = H;
	if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_READ, DB_LOCK_NOWAIT, &lk)) != 0)
		return (ret == DB_LOCK_DEADLOCK ? DB_LOCK_NOTGRANTED : ret);
	if ((ret = PAGEGET(dbc, mpf, &pgno, 0, &h)) != 0) {
		(void)__LPUT(dbc, lk);
		return (ret);
	}
	ptype = TYPE(h);
	level = LEVEL(h);
	if (ptype == P_LBTREE) {
		if (NUM_ENT(h) == 0)
			ret = DB_BTMOVE_STOP;	/* an empty leaf: not handled */
		else
			ret = __db_ret(dbp, h, 0, &key, &key.data, &key.ulen);
	} else if (ptype == P_INVALID) {
		ret = DB_NOTFOUND;	/* freed since planning: re-plan */
	} else if (ptype == P_IBTREE && !IS_PREFIX(h) && NUM_ENT(h) > 1 &&
	    B_TYPE(bi = GET_BINTERNAL(dbp, h, 1)) == B_KEYDATA) {
		/* Entry 1's key lies within H's range (entry 0's may not). */
		if ((ret = __os_malloc(dbenv, bi->len, &key.data)) == 0) {
			memcpy(key.data, bi->data, bi->len);
			key.size = bi->len;
		}
	} else
		ret = DB_BTMOVE_STOP;	/* e.g. an overflow page: not moved here */
	if ((t_ret = PAGEPUT(dbc, mpf, h, 0)) != 0 && ret == 0)
		ret = t_ret;
	h = NULL;
	(void)__LPUT(dbc, lk);
	LOCK_INIT(lk);
	if (ret != 0)
		goto err;

	/* Find H again with its parent, both write-locked. */
	if ((ret = __bam_search(dbc, PGNO_INVALID, &key,
	    S_PARENT | S_WRITE, level, NULL, &exact)) != 0)
		goto err;
	h = cp->csp->page;
	epg = &cp->csp[-1];
	if (cp->csp <= cp->sp || PGNO(h) != H || TYPE(h) != ptype ||
	    NUM_ENT(h) == 0 ||
	    GET_BINTERNAL(dbp, epg->page, epg->indx)->pgno != H) {
		ret = DB_NOTFOUND;
		goto err;
	}

	/* Lock order parent, H, prev, next; internal pages have no sibling links. */
	prev = ptype == P_LBTREE ? PREV_PGNO(h) : PGNO_INVALID;
	next = ptype == P_LBTREE ? NEXT_PGNO(h) : PGNO_INVALID;
	if (prev != PGNO_INVALID &&
	    ((ret = __db_lget(dbc, 0, prev, DB_LOCK_WRITE, 0, &plk)) != 0 ||
	    (ret = PAGEGET(dbc, mpf, &prev, 0, &pp)) != 0))
		goto err;
	if (next != PGNO_INVALID &&
	    ((ret = __db_lget(dbc, 0, next, DB_LOCK_WRITE, 0, &nlk)) != 0 ||
	    (ret = PAGEGET(dbc, mpf, &next, 0, &np)) != 0))
		goto err;
	if ((pp != NULL && NEXT_PGNO(pp) != H) ||
	    (np != NULL && PREV_PGNO(np) != H)) {
		ret = DB_NOTFOUND;
		goto err;
	}

	/* __db_new must hand us exactly L. */
	if (meta->free != L || L >= H || meta->last_pgno != H) {
		ret = DB_NOTFOUND;
		goto err;
	}
	if ((ret = __db_new(dbc, ptype, &lp)) != 0)
		goto err;
	if (PGNO(lp) != L) {
		ret = EINVAL;
		goto err;
	}
	pgno = L;
	if ((ret = __db_lget(dbc, 0, pgno, DB_LOCK_WRITE, 0, &lk)) != 0)
		goto err;
	dstlsn = LSN(lp);

	/* Logged in native byte order, like bam_pgcompact's image. */
	memset(&hdr, 0, sizeof(hdr));
	hdr.data = h;
	hdr.size = LOFFSET(dbp, h);
	memset(&data, 0, sizeof(data));
	data.data = (u_int8_t *)h + HOFFSET(h);
	data.size = dbp->pgsize - HOFFSET(h);
	if ((ret = __bam_pgmove_log(dbp, dbc->txn, &lsn, 0, H, L, &dstlsn,
	    &hdr, &data, PGNO(epg->page), &LSN(epg->page), epg->indx,
	    prev, pp != NULL ? &LSN(pp) : &LSN(meta),
	    next, np != NULL ? &LSN(np) : &LSN(meta))) != 0)
		goto err;
	logged = 1;

	/* Apply exactly what recovery would redo. */
	memset(&args, 0, sizeof(args));
	args.src = H;
	args.dst = L;
	args.dstlsn = dstlsn;
	args.hdr = hdr;
	args.data = data;
	args.ppgno = PGNO(epg->page);
	args.pindx = epg->indx;
	args.prev = prev;
	args.next = next;
	(void)__bam_pgmove_apply(dbp, lp, L, &args, &lsn, 1);
	(void)__bam_pgmove_apply(dbp, epg->page, args.ppgno, &args, &lsn, 1);
	if (pp != NULL)
		(void)__bam_pgmove_apply(dbp, pp, prev, &args, &lsn, 1);
	if (np != NULL)
		(void)__bam_pgmove_apply(dbp, np, next, &args, &lsn, 1);
	if ((ret = __memp_fset(mpf, epg->page, DB_MPOOL_DIRTY)) != 0)
		goto err;
	++GET_BH_GEN(epg->page);

	/* Rows now live on L: drop cached genid -> H entries. */
	__bam_pgmove_hash_clear(dbp, lp, H);
	if (t->bt_lpgno == H)
		t->bt_lpgno = PGNO_INVALID;

	/* Free H with its contents, so snapshot readers can rebuild it. */
	if (cp->page == h) {
		cp->page = NULL;
		LOCK_INIT(cp->lock);
	}
	ret = __db_free(dbc, h);
	cp->csp->page = NULL;
	h = NULL;

err:	if (lp != NULL && (t_ret = PAGEPUT(dbc, mpf, lp,
	    logged ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (pp != NULL && (t_ret = PAGEPUT(dbc, mpf, pp,
	    logged ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	if (np != NULL && (t_ret = PAGEPUT(dbc, mpf, np,
	    logged ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	/* Locks are kept to commit so replicas apply the move atomically. */
	(void)__TLPUT(dbc, lk);
	(void)__TLPUT(dbc, plk);
	(void)__TLPUT(dbc, nlk);
	if (cp->csp != NULL && cp->sp != NULL &&
	    (t_ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0 && ret == 0)
		ret = t_ret;
	if (key.data != NULL)
		__os_free(dbenv, key.data);
	if (ret == DB_LOCK_DEADLOCK)
		ret = DB_LOCK_NOTGRANTED;
	return (ret);
}
