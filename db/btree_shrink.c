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

/* Background thread for gradual btree shrinking; see bdb/shrink.c. */

#include <pthread.h>
#include <time.h>
#include <unistd.h>

#include "comdb2.h"
#include "bdb_api.h"
#include "schema_lk.h"
#include "sc_util.h"
#include "thrman.h"

int gbl_btree_shrink = 0;
int gbl_btree_shrink_sleep_ms = 10;
int gbl_btree_shrink_pass_interval_sec = 60;
int gbl_btree_shrink_truncate_interval_sec = 30;
extern int gbl_btree_shrink_move_blocked;
extern int gbl_default_sc_scanmode;

static pthread_t btree_shrink_tid;
static time_t last_truncate;

/* Cut trimmed pages from files, on any node, once no kept log needs them */
static void shrink_truncate_pass(void)
{
    uint32_t first, cur;

    rdlock_schema_lk();
    if (!get_schema_change_in_progress(__func__, __LINE__) &&
        bdb_shrink_truncate_begin(thedb->bdb_env, &first, &cur) == 0) {
        for (int i = 0; i < thedb->num_dbs && !db_is_exiting(); i++) {
            struct dbtable *db = thedb->dbs[i];
            if (db->dbtype == DBTYPE_TAGGED_TABLE && db->handle)
                bdb_shrink_truncate_table(db->handle, first, cur);
        }
        bdb_shrink_truncate_end(thedb->bdb_env);
    }
    unlock_schema_lk();
}

static void truncate_if_due(void)
{
    time_t now = time(NULL);
    if (now - last_truncate >= gbl_btree_shrink_truncate_interval_sec) {
        shrink_truncate_pass();
        last_truncate = now;
    }
}

static void shrink_sleep(int secs)
{
    for (int i = 0; i < secs && !db_is_exiting(); i++) {
        sleep(1);
        truncate_if_due();
    }
}

static void *btree_shrink_thread(void *arg)
{
    struct bdb_shrink_ctx *ctx = bdb_shrink_ctx_create();
    int tbl = 0;

    thrman_register(THRTYPE_GENERIC);
    thread_started("btree_shrink");
    backend_thread_event(thedb, COMDB2_THR_EVENT_START);

    while (!db_is_exiting()) {
        truncate_if_due();
        if (!gbl_btree_shrink || gbl_is_physical_replicant || thedb->master != gbl_myhostname) {
            bdb_shrink_ctx_reset(ctx);
            tbl = 0;
            shrink_sleep(1);
            continue;
        }

        rdlock_schema_lk();
        gbl_btree_shrink_move_blocked = (gbl_default_sc_scanmode == SCAN_PAGEORDER);
        /* Schema changes can't start mid-step: they take the schema write lock first */
        if (get_schema_change_in_progress(__func__, __LINE__)) {
            unlock_schema_lk();
            bdb_shrink_ctx_reset(ctx);
            shrink_sleep(1);
            continue;
        }
        if (tbl >= thedb->num_dbs) {
            unlock_schema_lk();
            tbl = 0;
            shrink_sleep(gbl_btree_shrink_pass_interval_sec);
            continue;
        }

        struct dbtable *db = thedb->dbs[tbl];
        int done = 1;
        if (db->dbtype == DBTYPE_TAGGED_TABLE && db->handle)
            done = bdb_shrink_table_step(db->handle, ctx);
        unlock_schema_lk();

        if (done)
            tbl++;
        if (gbl_btree_shrink_sleep_ms > 0)
            usleep(gbl_btree_shrink_sleep_ms * 1000);
    }

    bdb_shrink_ctx_destroy(ctx);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE);
    return NULL;
}

void create_btree_shrink_thread(void)
{
    Pthread_create(&btree_shrink_tid, &gbl_pthread_attr_detached, btree_shrink_thread, NULL);
}
