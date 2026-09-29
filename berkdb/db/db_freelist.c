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
