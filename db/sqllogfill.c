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
#include <cdb2api.h>
#include <sys_wrap.h>
#include <parse_lsn.h>
#include <log_info.h>
#include <comdb2.h>
#include <dbinc/rep_types.h>
#include <build/db_int.h>

/* Tunables */
int gbl_sql_logfills = 1;
int gbl_debug_sql_logfill = 1;
int gbl_sql_logfill_stats = 1;
int gbl_sql_logfill_only_gaps = 0;
int gbl_sql_logfill_dedicated_apply_thread = 1;
int gbl_sql_logfill_lookahead_records = 100000;
static int sql_logfill_thds_created = 0;

struct log_record {
    unsigned int file;
    unsigned int offset;
    int64_t rectype;
    void *blob;
    int blob_len;
    int blob_memsz;
};

struct log_record *apply_queue = NULL;
int apply_queue_head = 0;
int apply_queue_tail = 0;

/* Apply queue synchronization */
static pthread_mutex_t sql_apply_queue_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sql_apply_queue_cond = PTHREAD_COND_INITIALIZER;
static DB_LSN head_lsn = {0};
static int head_sz = 0;

/* Signal mechanism from berkley */
static pthread_t sql_logfill_thd;
static pthread_t sql_logfill_apply_thd;
static pthread_mutex_t sql_logfill_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t sql_logfill_cond = PTHREAD_COND_INITIALIZER;

/* Connection state */
static char *connected_node = NULL;
static cdb2_hndl_tp *hndl = NULL;
static int is_connected = 0;

#define SQL_CMD_LEN 200

/* Externs (to headerize) */
extern int db_is_exiting(void);
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern int bdb_get_lsn_node(bdb_state_type *bdb_state, char *host, uint32_t *logfile, uint32_t *offset);

/* Counters */
static int64_t records_applied = 0;
static int64_t bytes_applied = 0;
static int64_t finds = 0;
static int64_t nexts = 0;
static int64_t enque_blocks = 0;

static void disconnect_from_master(void)
{
    if (is_connected) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: disconnecting from %s\n", __func__, connected_node);
        }
        cdb2_close(hndl);
        hndl = NULL;
        is_connected = 0;
        connected_node = NULL;
    }
}

static int connect_to_master(bdb_state_type *bdb_state)
{
    int rc;
    char *master = thedb->master;

    if (is_connected && !strcmp(connected_node, master)) {
        return 0;
    }

    if (!master) {
        return 1;
    }

    if (is_connected) {
        disconnect_from_master();
    }

    rc = cdb2_open(&hndl, gbl_dbname, master, CDB2_DIRECT_CPU | CDB2_ADMIN);
    if (rc != 0) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: cdb2_open to master %s failed rc=%d\n", __func__,
                master, rc);
        }
        return 1;
    }

    /* Don't ping-pong here */
    rc = cdb2_run_statement(hndl, "select 1");
    if (rc != CDB2_OK) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_USER, "%s: cdb2_run_statement to master %s failed rc=%d\n",
                __func__, master, rc);
        }
        cdb2_close(hndl);
        hndl = NULL;
        return 1;
    }

    is_connected = 1;
    connected_node = master;

    if (gbl_debug_sql_logfill) {
        logmsg(LOGMSG_USER, "%s: connected to master %s\n", __func__, master);
    }

    return 0;
}

static int apply_queue_size()
{
    Pthread_mutex_lock(&sql_apply_queue_lock);
    int size = (apply_queue_head - apply_queue_tail + gbl_sql_logfill_lookahead_records) %
               gbl_sql_logfill_lookahead_records;
    Pthread_mutex_unlock(&sql_apply_queue_lock);
    return size;
}

static void enque_log_record(bdb_state_type *bdb_state, unsigned int file, unsigned int offset,
            int64_t rectype, void *blob, int blob_len)
{
    Pthread_mutex_lock(&sql_apply_queue_lock);
    if ((apply_queue_head + 1) % gbl_sql_logfill_lookahead_records == apply_queue_tail) {
        enque_blocks++;
    }

    while (((apply_queue_head + 1) % gbl_sql_logfill_lookahead_records) == apply_queue_tail &&
            !db_is_exiting() && !bdb_lock_desired(bdb_state)) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1;
        pthread_cond_timedwait(&sql_apply_queue_cond, &sql_apply_queue_lock, &ts);
    }

    if (db_is_exiting() || bdb_lock_desired(bdb_state)) {
        Pthread_mutex_unlock(&sql_apply_queue_lock);
        return;
    }

    struct log_record *rec = &apply_queue[apply_queue_head];

    rec->file = file;
    rec->offset = offset;
    rec->rectype = rectype;

    if (rec->blob_memsz < blob_len) {
        rec->blob = realloc(rec->blob, blob_len);
        rec->blob_memsz = blob_len;
    }
    memcpy(rec->blob, blob, blob_len);
    rec->blob_len = blob_len;

    apply_queue_head = (apply_queue_head + 1) % gbl_sql_logfill_lookahead_records;
    head_lsn.file = file;
    head_lsn.offset = offset;
    head_sz = blob_len;
    Pthread_cond_signal(&sql_apply_queue_cond);
    Pthread_mutex_unlock(&sql_apply_queue_lock);
}

static int handle_log(bdb_state_type *bdb_state, unsigned int file, unsigned int offset,
            int64_t rectype, void *blob, int blob_len)
{
    if (gbl_sql_logfill_dedicated_apply_thread) {
        enque_log_record(bdb_state, file, offset, rectype, blob, blob_len);
        return 0;
    }
    int rc = bdb_state->dbenv->apply_log(bdb_state->dbenv, file, offset, rectype, blob, blob_len);
    return rc;
}

static void printstats(void)
{
    static int lastpr = 0;
    int now = comdb2_time_epoch();
    if (now - lastpr > 0) {
        int qsz = apply_queue_size();
        logmsg(LOGMSG_USER, "sqllogfill records_applied=%" PRId64 " bytes_applied=%" PRId64
                        " finds=%" PRId64 " nexts=%" PRId64 " enque-blocks=%" PRId64 " qsz=%d\n",
               records_applied, bytes_applied, finds, nexts, enque_blocks, qsz);
        lastpr = now;
    }
}

static int apply_record(bdb_state_type *bdb_state, cdb2_hndl_tp *hndl, LOG_INFO *last_lsn, DB_LSN *gap_lsn)
{
    char *lsn;
    void *blob;
    DB_LSN mylsn = {0};
    int blob_len, rc;

    lsn = (char *)cdb2_column_value(hndl, 0);
    blob = cdb2_column_value(hndl, 4);
    blob_len = cdb2_column_size(hndl, 4);

    if ((rc = char_to_lsn(lsn, &mylsn.file, &mylsn.offset)) != 0) {
        if (gbl_debug_sql_logfill) {
            logmsg(LOGMSG_ERROR, "%s: char_to_lsn failed for lsn %s\n", __func__, lsn);
        }
        return rc;
    }

    if (last_lsn->file < mylsn.file) {
        rc = handle_log(bdb_state, last_lsn->file,
                get_next_offset(bdb_state->dbenv, *last_lsn), REP_NEWFILE, NULL, 0);
        if (rc != 0) {
            logmsg(LOGMSG_FATAL, "%s error applying newfile log record, %d\n", __func__, rc);
            exit(1);
        }
    }

    last_lsn->file = mylsn.file;
    last_lsn->offset = mylsn.offset;
    last_lsn->size = blob_len;

    /* Don't apply the gap: it should be applied already */
    if (!gap_lsn || log_compare(&mylsn, gap_lsn) < 0) {
        rc = handle_log(bdb_state, mylsn.file, mylsn.offset, REP_LOG, blob, blob_len);
        if (rc != 0 && rc != DB_REP_ISPERM) {
            logmsg(LOGMSG_FATAL, "%s error applying log record, %d\n", __func__, rc);
            exit(1);
        }
        records_applied++;
        bytes_applied+= blob_len;
    }
    return 0;
}

static void request_logs_from_master(bdb_state_type *bdb_state)
{
    DB_LSN next_lsn = {0}, gap_lsn = {0}, last_locked = {0}, master_lsn = {0};
    LOG_INFO last_lsn = {0};
    char sql_cmd[SQL_CMD_LEN];
    int nrecs = 0, rc;
    u_int32_t gen;

    while (!db_is_exiting() && !bdb_lock_desired(bdb_state)) {
 
        int have_gap = 1;

        /* Check for rep-verify-match */
        rc = bdb_state->dbenv->get_last_locked(bdb_state->dbenv, &last_locked, &gen);

        /* Returns non-0 before rep-verify-match */
        if (rc != 0) {
            return;
        }

        rc = connect_to_master(bdb_state);
        if (rc) {
            return;
        }

        rc = bdb_state->dbenv->get_rep_lsns(bdb_state->dbenv, &next_lsn, &gap_lsn, &nrecs);

        if (rc != 0) {
            return;
        }

        /* No gap */
        if (IS_ZERO_LSN(gap_lsn)) {
            have_gap = 0;
            if (gbl_sql_logfill_only_gaps)
                return;
        }

        bdb_get_lsn_node(bdb_state, connected_node, &master_lsn.file, &master_lsn.offset);

        /* Need last_lsn to detect newfile */
        if (gbl_sql_logfill_dedicated_apply_thread) {
            int get_last = 0;
            Pthread_mutex_lock(&sql_apply_queue_lock);
            if (apply_queue_head == apply_queue_tail) {
                get_last = 1;
                Pthread_mutex_unlock(&sql_apply_queue_lock);
            } else {
                last_lsn.file = head_lsn.file;
                last_lsn.offset = head_lsn.offset;
                last_lsn.size = head_sz;
                Pthread_mutex_unlock(&sql_apply_queue_lock);
                DB_LSN last_db_lsn = {last_lsn.file, last_lsn.offset};
                if (log_compare(&last_db_lsn, &gap_lsn) >= 0) {
                    return;
                }
            }
            if (get_last) {
                last_lsn = get_last_lsn(bdb_state);
            }
        } else {
            last_lsn = get_last_lsn(bdb_state);
        }

        /* Afraid this will race with normal replication */
        if (!have_gap) {
            DB_LSN last_db_lsn = {last_lsn.file, last_lsn.offset};
            if (log_compare(&last_db_lsn, &master_lsn) >= 0)
                return;
            gap_lsn = master_lsn;
        }

        /* TODO: could replace replication if we use BLOCKING */
        rc = snprintf(sql_cmd, SQL_CMD_LEN,
                "select * from comdb2_transaction_logs('{%u:%u}')",
                last_lsn.file, last_lsn.offset);
        if (rc < 0 || rc >= SQL_CMD_LEN) {
            logmsg(LOGMSG_ERROR, "%s: snprintf failed, rc=%d\n", __func__, rc);
            return;
        }

        if ((rc = cdb2_run_statement(hndl, sql_cmd)) != CDB2_OK) {
            if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_ERROR, "%s: cdb2_run_statement failed rc=%d\n", __func__, rc);
            }
            disconnect_from_master();
            return;
        }
        finds++;

        if ((rc = cdb2_next_record(hndl)) != CDB2_OK) {
            if (gbl_debug_sql_logfill) {
                logmsg(LOGMSG_USER, "%s: cdb2_next_record returned rc=%d\n", __func__, rc);
            }
            disconnect_from_master();
            return;
        }
        nexts++;

        while (!bdb_lock_desired(bdb_state) && !db_is_exiting() && 
               (rc = cdb2_next_record(hndl)) == CDB2_OK) {

            nexts++;
            rc = apply_record(bdb_state, hndl, &last_lsn, have_gap ? &gap_lsn : NULL);
            if (rc != 0) {
                logmsg(LOGMSG_FATAL, "%s: apply_record failed rc=%d\n", __func__, rc);
                exit(1);
            }

            DB_LSN last_db_lsn = {last_lsn.file, last_lsn.offset};

            if (log_compare(&last_db_lsn, &gap_lsn) >= 0) {
                break;
            }

            if (gbl_sql_logfill_stats) {
                printstats();
            }
        }
    }
}

static inline void sleep_for_gap_lsn(bdb_state_type *bdb_state)
{
    struct timespec ts;
    DB_LSN next_lsn = {0}, gap_lsn = {0};
    int nrecs = 0;

    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += 1;

    Pthread_mutex_lock(&sql_logfill_lock);
    bdb_state->dbenv->get_rep_lsns(bdb_state->dbenv, &next_lsn, &gap_lsn, &nrecs);
    if (IS_ZERO_LSN(gap_lsn)) {
        pthread_cond_timedwait(&sql_logfill_cond, &sql_logfill_lock, &ts);
    }
    Pthread_mutex_unlock(&sql_logfill_lock);
}

static void *sql_logfill_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;

    comdb2_name_thread(__func__);
    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_START_RDONLY);

    while (!db_is_exiting()) {
        BDB_READLOCK(__func__);

        if (thedb->master != gbl_myhostname) {
            request_logs_from_master(bdb_state);
        }

        BDB_RELLOCK();

        /* Wait for new master to be resolved */
        while (bdb_lock_desired(bdb_state) && !db_is_exiting()) {
            sleep(1);
        }

        /* sleep and recheck */
        sleep_for_gap_lsn(bdb_state);
    }

    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

static void *apply_thread(void *arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)arg;
    comdb2_name_thread(__func__);
    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_START_RDONLY);

    BDB_READLOCK(__func__);
    while(!db_is_exiting()) {

        Pthread_mutex_lock(&sql_apply_queue_lock);
        while (apply_queue_head == apply_queue_tail &&
                !bdb_lock_desired(bdb_state) &&
                !db_is_exiting()) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            pthread_cond_timedwait(&sql_apply_queue_cond, &sql_apply_queue_lock, &ts);
        }

        if (bdb_lock_desired(bdb_state)) {
            apply_queue_head = apply_queue_tail = 0;
            head_lsn = (DB_LSN){0};
        }

        Pthread_mutex_unlock(&sql_apply_queue_lock);

        if (bdb_lock_desired(bdb_state)) {
            BDB_RELLOCK();
            while(bdb_lock_desired(bdb_state) && !db_is_exiting()) {
                sleep(1);
            }
            BDB_READLOCK(__func__);
            continue;
        }

        struct log_record *rec = &apply_queue[apply_queue_tail];
        bdb_state->dbenv->apply_log(bdb_state->dbenv, rec->file, rec->offset,
                rec->rectype, rec->blob, rec->blob_len);

        Pthread_mutex_lock(&sql_apply_queue_lock);
        apply_queue_tail = (apply_queue_tail + 1) % gbl_sql_logfill_lookahead_records;
        Pthread_cond_signal(&sql_apply_queue_cond);
        Pthread_mutex_unlock(&sql_apply_queue_lock);
    }
    BDB_RELLOCK();
    bdb_thread_event(bdb_state, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}

static void create_apply_queue(bdb_state_type *bdb_state)
{
    apply_queue = calloc(gbl_sql_logfill_lookahead_records, sizeof(struct log_record));
    apply_queue_head = apply_queue_tail = 0;
}

static void create_logfill_threads(bdb_state_type *bdb_state)
{
    Pthread_mutex_lock(&sql_logfill_lock);
    if (!sql_logfill_thds_created && gbl_sql_logfills) {
        if (gbl_sql_logfill_dedicated_apply_thread) {
            create_apply_queue(bdb_state);
        }
        Pthread_create(&sql_logfill_thd, NULL, sql_logfill_thread, bdb_state);
        if (gbl_sql_logfill_dedicated_apply_thread) {
            Pthread_create(&sql_logfill_apply_thd, NULL, apply_thread, bdb_state);
        }
        sql_logfill_thds_created = 1;
    }
    Pthread_cond_signal(&sql_logfill_cond);
    Pthread_mutex_unlock(&sql_logfill_lock);
}

void sql_logfill_signal(bdb_state_type *bdb_state)
{
    create_logfill_threads(bdb_state);
}

void create_sql_logfill_threads(bdb_state_type *bdb_state)
{
    create_logfill_threads(bdb_state);
}

