/*
   Copyright 2025 Bloomberg Finance L.P.

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
#include <sqllogfill.h>
#include <sys_wrap.h>

int gbl_sql_logfills = 1;

static int sql_logfill_thd_created = 0;
static pthread_t sql_logfill_thd;
static pthread_mutex_t sql_logfill_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sql_logfill_cond = PTHREAD_COND_INITIALIZER;

// For debug trace
static int signal_start_file = -1, signal_start_offset = -1;
static int signal_end_file = -1, signal_end_offset = -1;

static void sql_logfill_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
}

void sql_logfill_signal(bdb_state_type *bdb_state, int sfile, int soffset, int efile, int eoffset)
{
    Pthread_mutex_lock(&sql_logfill_lock);
    if (!sql_logfill_thd_created && gbl_sql_logfills) {
        sql_logfill_thd_created = 1;
        Pthread_create(&sql_logfill_thd, NULL, sql_logfill_thread, bdb_state);
    }

    signal_start_file = sfile;
    signal_start_offset = soffset;
    signal_end_file = efile;
    signal_end_offset = eoffset;

    Pthread_cond_signal(&sql_logfill_cond);
    Pthread_mutex_unlock(&sql_logfill_lock);
}
