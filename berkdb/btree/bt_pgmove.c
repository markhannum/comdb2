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

/* Owner hints: chain head -> owning key, from one leaf scan; rewrites recheck them. */
#define	OVMAP_EMPTY	PGNO_INVALID
#define	OVMAP_TOMB	((db_pgno_t)-1)
#define	OVMAP_KEYCHAIN	0x80000000	/* in len: the chain belongs to a key */

struct __db_ovmap {
	DB_ENV *dbenv;
	u_int8_t fileid[DB_FILE_ID_LEN];
	struct ovmap_ent {
		db_pgno_t head;
		u_int32_t off;
		u_int32_t len;
	} *tab;
	u_int32_t size;		/* slots, a power of 2 */
	u_int32_t used;		/* live entries and tombstones */
	u_int8_t *keys;
	size_t keysz, keyused;
	int full;		/* hit btree_shrink_ovmap_max_mb */
};

int gbl_btree_shrink_ovmap_max_mb = 64;
u_int64_t gbl_btree_shrink_ovscans;	/* full leaf scans for an owner */
u_int64_t gbl_btree_shrink_ovhits;	/* owners found in the map */

static int
ovmap_grow(struct __db_ovmap *m)
{
	struct ovmap_ent *old, *e;
	u_int32_t i, j, osize;
	int ret;

	osize = m->size;
	old = m->tab;
	if (gbl_btree_shrink_ovmap_max_mb <= 0 ||
	    (size_t)osize * 2 * sizeof(*old) + m->keysz >
	    (size_t)gbl_btree_shrink_ovmap_max_mb << 20) {
		m->full = 1;
		return (ENOMEM);
	}
	if ((ret = __os_calloc(m->dbenv,
	    osize * 2, sizeof(*old), &m->tab)) != 0) {
		m->tab = old;
		return (ret);
	}
	m->size = osize * 2;
	m->used = 0;
	for (i = 0; i < osize; i++) {
		if (old[i].head == OVMAP_EMPTY || old[i].head == OVMAP_TOMB)
			continue;
		for (j = old[i].head & (m->size - 1);; j = (j + 1) & (m->size - 1))
			if ((e = &m->tab[j])->head == OVMAP_EMPTY)
				break;
		*e = old[i];
		m->used++;
	}
	__os_free(m->dbenv, old);
	return (0);
}

static struct ovmap_ent *
ovmap_find(struct __db_ovmap *m, db_pgno_t head)
{
	struct ovmap_ent *e;
	u_int32_t j;

	for (j = head & (m->size - 1);; j = (j + 1) & (m->size - 1)) {
		e = &m->tab[j];
		if (e->head == head)
			return (e);
		if (e->head == OVMAP_EMPTY)
			return (NULL);
	}
}

/* Add head's owner: len bytes at off in the arena (already stored). */
static int
ovmap_put(struct __db_ovmap *m, db_pgno_t head, u_int32_t off, u_int32_t len)
{
	struct ovmap_ent *e;
	u_int32_t j;
	int ret;

	if ((e = ovmap_find(m, head)) != NULL) {
		e->off = off;
		e->len = len;
		return (0);
	}
	/* At most half full, so probes stay short and always end. */
	if ((m->used + 1) * 2 > m->size && (ret = ovmap_grow(m)) != 0)
		return (ret);
	for (j = head & (m->size - 1);; j = (j + 1) & (m->size - 1)) {
		e = &m->tab[j];
		if (e->head == OVMAP_EMPTY || e->head == OVMAP_TOMB)
			break;
	}
	if (e->head == OVMAP_EMPTY)
		m->used++;
	e->head = head;
	e->off = off;
	e->len = len;
	return (0);
}

static int
ovmap_add(struct __db_ovmap *m, db_pgno_t head, DBT *key, int keychain)
{
	size_t need;
	u_int32_t off;
	int ret;

	if (m->full)
		return (ENOMEM);
	if (keychain)
		return (ovmap_put(m, head, 0, OVMAP_KEYCHAIN));
	if (m->keyused + key->size > UINT32_MAX) {	/* offsets are 32-bit */
		m->full = 1;
		return (ENOMEM);
	}
	if (m->keyused + key->size > m->keysz) {
		need = m->keysz ? m->keysz * 2 : 4096;
		while (need < m->keyused + key->size)
			need *= 2;
		if (gbl_btree_shrink_ovmap_max_mb <= 0 ||
		    m->size * sizeof(m->tab[0]) + need >
		    (size_t)gbl_btree_shrink_ovmap_max_mb << 20) {
			m->full = 1;
			return (ENOMEM);
		}
		if ((ret = __os_realloc(m->dbenv, need, &m->keys)) != 0)
			return (ret);
		m->keysz = need;
	}
	off = (u_int32_t)m->keyused;
	memcpy(m->keys + off, key->data, key->size);
	m->keyused += key->size;
	return (ovmap_put(m, head, off, key->size));
}

static void
ovmap_clear(struct __db_ovmap *m)
{
	memset(m->tab, 0, m->size * sizeof(m->tab[0]));
	m->used = 0;
	m->keyused = 0;
	m->full = 0;
}

/*
 * __db_ovmap_create -- create an empty owner map for dbp.
 * PUBLIC: int __db_ovmap_create __P((DB *, struct __db_ovmap **));
 */
int
__db_ovmap_create(DB *dbp, struct __db_ovmap **mp)
{
	struct __db_ovmap *m;
	int ret;

	*mp = NULL;
	if ((ret = __os_calloc(dbp->dbenv, 1, sizeof(*m), &m)) != 0)
		return (ret);
	m->dbenv = dbp->dbenv;
	memcpy(m->fileid, dbp->fileid, DB_FILE_ID_LEN);
	m->size = 1024;
	if ((ret = __os_calloc(dbp->dbenv,
	    m->size, sizeof(m->tab[0]), &m->tab)) != 0) {
		__os_free(dbp->dbenv, m);
		return (ret);
	}
	*mp = m;
	return (0);
}

/*
 * __db_ovmap_destroy -- free an owner map.
 * PUBLIC: void __db_ovmap_destroy __P((struct __db_ovmap *));
 */
void
__db_ovmap_destroy(struct __db_ovmap *m)
{
	if (m == NULL)
		return;
	if (m->keys != NULL)
		__os_free(m->dbenv, m->keys);
	__os_free(m->dbenv, m->tab);
	__os_free(m->dbenv, m);
}

/*
 * __db_ovmap_matches -- is m the owner map of dbp?
 * PUBLIC: int __db_ovmap_matches __P((struct __db_ovmap *, DB *));
 */
int
__db_ovmap_matches(struct __db_ovmap *m, DB *dbp)
{
	return (memcmp(m->fileid, dbp->fileid, DB_FILE_ID_LEN) == 0);
}

/*
 * __bam_ovmap_moved -- the chain at oldhead now starts at newhead (or is gone).
 * PUBLIC: void __bam_ovmap_moved __P((struct __db_ovmap *,
 * PUBLIC:     db_pgno_t, db_pgno_t));
 */
void
__bam_ovmap_moved(struct __db_ovmap *m, db_pgno_t oldhead, db_pgno_t newhead)
{
	struct ovmap_ent *e;
	u_int32_t off, len;

	if (m == NULL || (e = ovmap_find(m, oldhead)) == NULL)
		return;
	off = e->off;
	len = e->len;
	e->head = OVMAP_TOMB;
	if (newhead != PGNO_INVALID)
		(void)ovmap_put(m, newhead, off, len);
}

/*
 * __bam_ovowner -- find head's owning key, from map or a scan of the leaves.
 * PUBLIC: int __bam_ovowner __P((DB *, struct __db_ovmap *,
 * PUBLIC:     db_pgno_t, DBT *, int *, int *));
 */
int
__bam_ovowner(DB *dbp, struct __db_ovmap *map, db_pgno_t head, DBT *key,
    int *keychainp, int *scannedp)
{
	BKEYDATA *bk;
	DBC *dbc;
	DBT k;
	DB_LOCK lk;
	DB_MPOOLFILE *mpf;
	PAGE *h;
	struct ovmap_ent *e;
	db_pgno_t pgno, bpgno;
	u_int32_t i;
	int found, ret, t_ret;

	mpf = dbp->mpf;
	h = NULL;
	found = 0;
	*keychainp = 0;
	*scannedp = 0;
	LOCK_INIT(lk);
	memset(&k, 0, sizeof(k));

	/* Key-chain hints aren't checked by a rewrite, so rescan for those. */
	if (map != NULL && (e = ovmap_find(map, head)) != NULL &&
	    !(e->len & OVMAP_KEYCHAIN)) {
		++gbl_btree_shrink_ovhits;
		if (key->ulen < e->len && (ret = __os_realloc(dbp->dbenv,
		    e->len, &key->data)) != 0)
			return (ret);
		if (key->ulen < e->len)
			key->ulen = e->len;
		memcpy(key->data, map->keys + e->off, e->len);
		key->size = e->len;
		return (0);
	}

	*scannedp = 1;
	++gbl_btree_shrink_ovscans;
	if (map != NULL)
		ovmap_clear(map);
	if ((ret = __db_cursor(dbp, NULL, &dbc, 0)) != 0)
		return (ret);

	/* Down the left edge to the first leaf. */
	for (pgno = dbc->internal->root;;) {
		if ((ret = __db_lget(dbc, LCK_COUPLE,
		    pgno, DB_LOCK_READ, DB_LOCK_NOWAIT, &lk)) != 0 ||
		    (ret = __memp_fget(mpf, &pgno, 0, &h)) != 0)
			goto err;
		if (TYPE(h) == P_LBTREE)
			break;
		if (TYPE(h) != P_IBTREE || NUM_ENT(h) == 0) {
			ret = DB_NOTFOUND;
			goto err;
		}
		pgno = GET_BINTERNAL(dbp, h, 0)->pgno;
		(void)__memp_fput(mpf, h, 0);
		h = NULL;
	}

	/* Across the leaves; to the end when there's a map to fill. */
	for (;;) {
		for (i = 0; i < NUM_ENT(h); i++) {
			bk = GET_BKEYDATA(dbp, h, i);
			if (B_TYPE(bk) != B_OVERFLOW)
				continue;
			memcpy(&bpgno, &((BOVERFLOW *)bk)->pgno, sizeof(bpgno));
			if (i % P_INDX == 0) {
				if (bpgno == head)
					*keychainp = 1;
				if (map != NULL && !map->full)
					(void)ovmap_add(map, bpgno, NULL, 1);
				continue;
			}
			if (bpgno != head && (map == NULL || map->full))
				continue;
			if ((ret = __db_ret(dbp, h, i - O_INDX,
			    &k, &k.data, &k.ulen)) != 0)
				goto err;
			if (map != NULL && !map->full)
				(void)ovmap_add(map, bpgno, &k, 0);
			if (bpgno == head) {
				if (key->ulen < k.size && (ret = __os_realloc(
				    dbp->dbenv, k.size, &key->data)) != 0)
					goto err;
				if (key->ulen < k.size)
					key->ulen = k.size;
				memcpy(key->data, k.data, k.size);
				key->size = k.size;
				found = 1;
			}
		}
		if (((found || *keychainp) && (map == NULL || map->full)) ||
		    (pgno = NEXT_PGNO(h)) == PGNO_INVALID)
			break;
		(void)__memp_fput(mpf, h, 0);
		h = NULL;
		if ((ret = __db_lget(dbc, LCK_COUPLE,
		    pgno, DB_LOCK_READ, DB_LOCK_NOWAIT, &lk)) != 0 ||
		    (ret = __memp_fget(mpf, &pgno, 0, &h)) != 0)
			goto err;
		if (TYPE(h) != P_LBTREE) {
			ret = DB_NOTFOUND;
			goto err;
		}
	}
	if (!found && !*keychainp)
		ret = DB_NOTFOUND;

err:	/* A busy page after the owner was found only cuts the refill short */
	if (ret != 0 && ret != DB_NOTFOUND && found)
		ret = 0;
	if (found)
		*keychainp = 0;
	if (h != NULL)
		(void)__memp_fput(mpf, h, 0);
	(void)__LPUT(dbc, lk);
	if ((t_ret = __db_c_close(dbc)) != 0 && ret == 0)
		ret = t_ret;
	if (k.data != NULL)
		__os_free(dbp->dbenv, k.data);
	return (ret == DB_LOCK_DEADLOCK ? DB_LOCK_NOTGRANTED : ret);
}

/*
 * __bam_ovrewrite -- copy key's chain (holding H) into want[], then free the old one.
 * PUBLIC: int __bam_ovrewrite __P((DBC *, DBT *, db_pgno_t,
 * PUBLIC:     db_pgno_t *, u_int32_t, u_int32_t, db_pgno_t *,
 * PUBLIC:     u_int32_t *, db_pgno_t *, int *));
 */
int
__bam_ovrewrite(DBC *dbc, DBT *key, db_pgno_t H, db_pgno_t *want,
    u_int32_t nwant, u_int32_t maxpages, db_pgno_t *old, u_int32_t *npagesp,
    db_pgno_t *newheadp, int *stopp)
{
	BKEYDATA *bk;
	BOVERFLOW bo;
	BTREE_CURSOR *cp;
	DB *dbp;
	DB_ENV *dbenv;
	DB_LOCK metalock;
	DB_MPOOLFILE *mpf;
	DBMETA *meta;
	DBT dbt, hdr;
	PAGE *h, *p;
	db_indx_t indx;
	db_pgno_t head, newpgno, pg, next;
	u_int32_t bufsz, i, n, tlen;
	void *buf;
	int exact, member, ret, t_ret;

	dbp = dbc->dbp;
	dbenv = dbp->dbenv;
	mpf = dbp->mpf;
	cp = (BTREE_CURSOR *)dbc->internal;
	meta = NULL;
	buf = NULL;
	bufsz = 0;
	LOCK_INIT(metalock);
	*npagesp = 0;
	*stopp = 0;

	if (!DBC_LOGGING(dbc))
		return (EPERM);
	if (F_ISSET(dbp, DB_AM_RECNUM | DB_AM_DUP)) {
		*stopp = DB_BTMOVE_STOP_PAGE;
		return (DB_BTMOVE_STOP);
	}

	/* The owning leaf, write-locked until commit. */
	BT_STK_CLR(cp);
	if ((ret = __bam_search(dbc, PGNO_INVALID, key,
	    S_FIND_WR | S_EXACT, LEAFLEVEL, NULL, &exact)) != 0)
		goto err;
	h = cp->csp->page;
	indx = cp->csp->indx + O_INDX;
	if (TYPE(h) != P_LBTREE || indx >= NUM_ENT(h)) {
		ret = DB_NOTFOUND;
		goto err;
	}
	bk = GET_BKEYDATA(dbp, h, indx);
	if (B_TYPE(bk) != B_OVERFLOW || B_DISSET(bk)) {
		ret = DB_NOTFOUND;
		goto err;
	}
	memcpy(&head, &((BOVERFLOW *)bk)->pgno, sizeof(head));
	memcpy(&tlen, &((BOVERFLOW *)bk)->tlen, sizeof(tlen));

	/* The chain is protected by the leaf lock: it must still hold H. */
	for (pg = head, n = 0, member = 0; pg != PGNO_INVALID; n++) {
		if (n >= maxpages) {
			logmsg(LOGMSG_WARN, "btree_shrink OVLONG diag: owner "
			    "walk %s H %u head %u walked %u cap %u tlen %u "
			    "expected pages %u at %u\n",
			    dbp->fname ? dbp->fname : "?", H, head, n,
			    maxpages, tlen, (tlen + P_MAXSPACE(dbp,
			    dbp->pgsize) - 1) / P_MAXSPACE(dbp, dbp->pgsize),
			    pg);
			*stopp = DB_BTMOVE_STOP_OVLONG;
			ret = DB_BTMOVE_STOP;
			goto err;
		}
		old[n] = pg;
		if ((ret = __memp_fget(mpf, &pg, 0, &p)) != 0)
			goto err;
		if (TYPE(p) != P_OVERFLOW)
			ret = DB_NOTFOUND;
		else if (n == 0 && OV_REF(p) != 1) {
			*stopp = DB_BTMOVE_STOP_OVKEY;
			ret = DB_BTMOVE_STOP;
		}
		next = NEXT_PGNO(p);
		(void)__memp_fput(mpf, p, 0);
		if (ret != 0)
			goto err;
		if (pg == H)
			member = 1;
		pg = next;
	}
	/* __db_poff must need exactly the n pages we plan for it. */
	if (!member || n != (tlen + P_MAXSPACE(dbp, dbp->pgsize) - 1) /
	    P_MAXSPACE(dbp, dbp->pgsize)) {
		ret = DB_NOTFOUND;
		goto err;
	}
	if (n > nwant) {
		*stopp = DB_BTMOVE_STOP_NOROOM;
		ret = DB_BTMOVE_STOP;
		goto err;
	}

	memset(&dbt, 0, sizeof(dbt));
	if ((ret = __db_goff(dbc, dbp, &dbt, tlen, head, &buf, &bufsz)) != 0)
		goto err;

	/* Meta last; holding it, __db_poff pops exactly the planned pages. */
	pg = PGNO_BASE_MD;
	if ((ret = __db_lget(dbc,
	    LCK_ALWAYS, pg, DB_LOCK_WRITE, 0, &metalock)) != 0 ||
	    (ret = PAGEGET(dbc, mpf, &pg, 0, &meta)) != 0)
		goto err;
	if (meta->last_pgno != H) {
		ret = DB_BTMOVE_PLAN;
		goto err;
	}
	for (i = 0, pg = meta->free; i < n; i++) {
		if (pg != want[i] || (ret = __memp_fget(mpf, &pg, 0, &p)) != 0) {
			if (ret == 0)
				ret = DB_BTMOVE_PLAN;
			goto err;
		}
		if (TYPE(p) != P_INVALID)
			ret = DB_BTMOVE_PLAN;
		next = NEXT_PGNO(p);
		(void)__memp_fput(mpf, p, 0);
		if (ret != 0)
			goto err;
		pg = next;
	}

	/* New chain first, from the lowest free pages; then drop the old. */
	if ((ret = __db_poff(dbc, &dbt, &newpgno)) != 0)
		goto err;
	for (i = 0, pg = newpgno; i < n; i++) {
		if (pg != want[i] || (ret = __memp_fget(mpf, &pg, 0, &p)) != 0) {
			if (ret == 0)
				ret = EINVAL;
			goto err;
		}
		next = NEXT_PGNO(p);
		(void)__memp_fput(mpf, p, 0);
		pg = next;
	}
	if (pg != PGNO_INVALID) {
		ret = EINVAL;
		goto err;
	}
	if ((ret = __bam_ditem(dbc, h, indx)) != 0)
		goto err;
	memset(&bo, 0, sizeof(bo));
	B_TSET(&bo, B_OVERFLOW, 0, 0, 0);
	bo.pgno = newpgno;
	bo.tlen = tlen;
	memset(&hdr, 0, sizeof(hdr));
	hdr.data = &bo;
	hdr.size = BOVERFLOW_SIZE;
	if ((ret = __db_pitem(dbc, h, indx, BOVERFLOW_SIZE, &hdr, NULL)) != 0)
		goto err;
	if ((ret = __memp_fset(mpf, h, DB_MPOOL_DIRTY)) != 0)
		goto err;
	*npagesp = n;
	*newheadp = newpgno;

err:	if (meta != NULL && (t_ret = PAGEPUT(dbc, mpf, meta,
	    ret == 0 ? DB_MPOOL_DIRTY : 0)) != 0 && ret == 0)
		ret = t_ret;
	/* Locks are kept to commit so replicas apply the rewrite atomically. */
	(void)__TLPUT(dbc, metalock);
	if (buf != NULL)
		__os_free(dbenv, buf);
	if ((t_ret = __bam_stkrel(dbc, STK_CLRDBC)) != 0 && ret == 0)
		ret = t_ret;
	return (ret == DB_LOCK_DEADLOCK ? DB_LOCK_NOTGRANTED : ret);
}
