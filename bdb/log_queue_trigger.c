/*
 * Rebuild queue-payload from transaction log
 */
#include <log_trigger.h>
#include <log_queue_trigger.h>
#include <bdb_int.h>
#include <assert.h>
#include <db_int.h>
#include <dbinc/btree.h>
#include <build/db.h>
#include <dbinc_auto/btree_auto.h>

#define MAX_QUEUE_FILES 64

#define FLAGS_BIG 1

static comdb2ma qtlogqma;
int gbl_log_queue_trigger_debug = 0;

static comdb2ma logqma(void)
{
    if (!qtlogqma) {
        qtlogqma = comdb2ma_create(0, 0, "log_queue_triggers", 0);
    }
    return qtlogqma;
}

struct log_queue_record {
    u_int64_t txnid;
    u_int8_t key[12];
    u_int8_t *record;
    void *freeptr;
    struct odh odh;
    int record_len;
    int offset;
    int bigcnt;
    u_int32_t flags;
    LINKC_T(struct log_queue_record) lnk;
};

struct log_queue_file {
    char *filename;
    bdb_state_type *bdb_state;
    pthread_mutex_t hash_lk;
    pthread_mutex_t queue_lk;
    pthread_cond_t queue_cd;
    hash_t *transactions;
    void (*func)(const char *filename, DBT *key, DBT *data, void *userptr);
    void *userptr;
    bdb_state_type *(*gethndl)(const char *q);
    LISTC_T(struct log_queue_record) outqueue;
    pthread_t consumer;
};

static int current_queue_files = 0;
struct log_queue_file log_queue_files[MAX_QUEUE_FILES] = {0};

static void *qmalloc(size_t sz)
{
    return comdb2_malloc(logqma(), sz);
}

static void qfree(void *ptr)
{
    comdb2_free(ptr);
}

/* Lockless file lookup */
static inline struct log_queue_file *retrieve_log_queue(const char *filename)
{
    for (int i = 0; i < current_queue_files; i++) {
        if (strcmp(log_queue_files[i].filename, filename) == 0) {
            return &log_queue_files[i];
        }
    }
    return NULL;
}

static struct log_queue_file *add_log_queue(const char *filename, bdb_state_type *(*gethndl)(const char *q),
                                            void (*func)(const char *filename, DBT *key, DBT *data, void *userptr),
                                            void *userptr)
{
    struct log_queue_file *file = retrieve_log_queue(filename);
    if (!file) {
        if (current_queue_files >= MAX_QUEUE_FILES) {
            logmsg(LOGMSG_ERROR, "%s out of queue-files\n", __func__);
            return NULL;
        }

        log_queue_files[current_queue_files].filename = comdb2_strdup(logqma(), filename);
        log_queue_files[current_queue_files].transactions = hash_setalloc_init(qmalloc, qfree, 0, sizeof(u_int64_t));
        listc_init(&log_queue_files[current_queue_files].outqueue, offsetof(struct log_queue_record, lnk));

        Pthread_mutex_init(&log_queue_files[current_queue_files].hash_lk, NULL);
        Pthread_mutex_init(&log_queue_files[current_queue_files].queue_lk, NULL);
        Pthread_cond_init(&log_queue_files[current_queue_files].queue_cd, NULL);
        file = &log_queue_files[current_queue_files++];
    }

    if (file) {
        file->gethndl = gethndl;
        file->func = func;
        file->userptr = userptr;
    }

    if (gbl_log_queue_trigger_debug) {
        logmsg(LOGMSG_USER, "%s looking for %s returning %p\n", __func__, filename, file);
    }
    return file;
}

static void log_queue_record_free(struct log_queue_record *txn)
{
    comdb2_free(txn->freeptr);
    comdb2_free(txn->record);
    comdb2_free(txn);
}

static int log_queue_record_free_hash(void *obj, void *arg)
{
    struct log_queue_record *txn = obj;
    log_queue_record_free(txn);
    return 0;
}

extern int bdb_unpack_updateid(bdb_state_type *bdb_state, const void *from, size_t fromlen, void *to, size_t tolen,
                               struct odh *odh, int updateid, void **freeptr, int verify_updateid, int force_odh,
                               void *(*fn_malloc)(size_t), void (*fn_free)(void *));

static int unpack_and_queue(struct log_queue_file *file, struct log_queue_record *txn)
{
    if (file->bdb_state == NULL) {
        if (file->gethndl) {
            file->bdb_state = file->gethndl(file->filename);
        }
        if (!file->bdb_state) {
            logmsg(LOGMSG_ERROR, "Could not find %s\n", file->filename);
            log_queue_record_free(txn);
            return -1;
        }
    }
    bdb_unpack_updateid(file->bdb_state, txn->record, txn->record_len, NULL, 0, &txn->odh, 0, &txn->freeptr, 0, 0,
                        qmalloc, qfree);
    Pthread_mutex_lock(&file->queue_lk);
    listc_abl(&file->outqueue, txn);
    Pthread_cond_signal(&file->queue_cd);
    Pthread_mutex_unlock(&file->queue_lk);
    return 0;
}

static int handle_big(struct log_queue_file *file, u_int64_t txnid, const __db_big_args *argp)
{
    Pthread_mutex_lock(&file->hash_lk);
    struct log_queue_record *txn = hash_find(file->transactions, &txnid);
    if (!txn) {
        abort();
    }
    assert(txn);
    txn->bigcnt++;
    txn->flags |= FLAGS_BIG;
    if (gbl_log_queue_trigger_debug) {
        logmsg(LOGMSG_USER, "%s txnid=%lu bigcnt=%d\n", __func__, txnid, txn->bigcnt);
    }
    txn->record = comdb2_realloc(logqma(), txn->record, txn->record_len + argp->dbt.size);
    memcpy(txn->record + txn->record_len, argp->dbt.data, argp->dbt.size);
    txn->record_len += argp->dbt.size;
    Pthread_mutex_unlock(&file->hash_lk);
    return 0;
}

static int handle_addrem(struct log_queue_file *file, u_int64_t txnid, const __db_addrem_args *argp)
{
    Pthread_mutex_lock(&file->hash_lk);
    struct log_queue_record *txn = hash_find(file->transactions, &txnid);
    if (txn == NULL) {

        assert(argp->indx % 2 == 0);
        assert(argp->dbt.size == 12);

        if (gbl_log_queue_trigger_debug) {
            logmsg(LOGMSG_USER, "%s txnid=%lu initial, idx=%d size=%d\n", __func__, txnid, argp->indx, argp->dbt.size);
        }

        txn = comdb2_calloc(logqma(), sizeof(struct log_queue_record), 1);
        txn->txnid = txnid;
        memcpy(txn->key, argp->dbt.data, 12);
        hash_add(file->transactions, txn);
        Pthread_mutex_unlock(&file->hash_lk);
        return 0;
    }

    // Either short-data case or we've finished collecting overflow records
    // and are updating original leaf-page
    assert(argp->indx % 2 == 1);
    if (!(txn->flags & FLAGS_BIG)) {
        txn->record = comdb2_malloc(logqma(), argp->dbt.size);
        txn->record_len = argp->dbt.size;
        memcpy(txn->record, argp->dbt.data, argp->dbt.size);
    }
    hash_del(file->transactions, txn);
    Pthread_mutex_unlock(&file->hash_lk);

    if (gbl_log_queue_trigger_debug) {
        logmsg(LOGMSG_USER, "%s txnid=%lu final idx=%d size=%d\n", __func__, txnid, argp->indx, argp->dbt.size);
    }
    return unpack_and_queue(file, txn);
}

static inline int nameboundry(char c)
{
    switch (c) {
    case '/':
    case '.':
        return 1;
    default:
        return 0;
    }
}

static int log_queue_trigger_callback(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *infilename,
                                      u_int32_t rectype, const void *log)
{
    char *mfile = alloca(strlen(infilename) + 1);
    strcpy(mfile, infilename);

    char *start = (char *)&mfile[0];
    char *end = (char *)&mfile[strlen(infilename) - 1];

    while (end > start && *end != '\0' && *end != '.') {
        end--;
    }
    end -= 17;
    if (end <= start) {
        logmsg(LOGMSG_ERROR, "%s invalid file %s\n", __func__, infilename);
        return -1;
    }

    char *fstart = end, *filename = NULL;
    while (fstart > start && (end - fstart) <= MAXTABLELEN) {
        if (nameboundry(*(fstart - 1))) {
            end[0] = '\0';
            filename = fstart;
            break;
        }
        fstart--;
    }

    struct log_queue_file *file;
    if (!filename || (file = retrieve_log_queue(filename)) == NULL) {
        return -1;
    }

    int rc = normalize_rectype(&rectype);
    if (rectype > 1000 && rectype < 10000) {
        rectype = rectype - 1000;
    }

    switch (rectype) {
    case DB___db_addrem: {
        const __db_addrem_args *argp = log;
        u_int64_t txnid = rc ? argp->txnid->utxnid : argp->txnid->txnid;
        if (argp->opcode == DB_ADD_DUP) {
            return handle_addrem(file, txnid, argp);
        }
    }
    case DB___db_big: {
        const __db_big_args *argp = log;
        u_int64_t txnid = rc ? argp->txnid->utxnid : argp->txnid->txnid;
        if (argp->opcode == DB_ADD_BIG) {
            return handle_big(file, txnid, argp);
        }
    }
    default:
        return 0;
    }
}

void *log_queue_consumer(void *arg)
{
    struct log_queue_file *file = arg;
    while (1) {
        Pthread_mutex_lock(&file->queue_lk);
        while (listc_size(&file->outqueue) == 0) {
            Pthread_cond_wait(&file->queue_cd, &file->queue_lk);
        }
        struct log_queue_record *txn = listc_rtl(&file->outqueue);
        Pthread_mutex_unlock(&file->queue_lk);
        DBT key = {.data = txn->key, .size = 12};
        DBT data = {.data = txn->odh.recptr, .size = txn->odh.length};
        if (file->func) {
            file->func(file->filename, &key, &data, file->userptr);
        }
        log_queue_record_free(txn);
    }
}

/* Clear all log-queues */
void clear_log_queues(void)
{
    for (int i = 0; i < current_queue_files; i++) {
        struct log_queue_file *file = &log_queue_files[i];
        Pthread_mutex_lock(&file->hash_lk);
        hash_for(file->transactions, log_queue_record_free_hash, NULL);
        hash_clear(file->transactions);
        Pthread_mutex_unlock(&file->hash_lk);
        Pthread_mutex_lock(&file->queue_lk);
        struct log_queue_record *txn;
        while ((txn = listc_rtl(&file->outqueue)) != NULL) {
            log_queue_record_free(txn);
        }
        Pthread_mutex_unlock(&file->queue_lk);
    }
}

/* TODO: Register another callback to invoke from here?
 * 
 * Two possible paths: active will decode & enqueue to kafka, or something
 * Passive can use already existing code - substituting this record from 
 * the berkley calls in dbq-get.  */

int register_logqueue_trigger(const char *filename, bdb_state_type *(*gethndl)(const char *q),
        void (*func)(const char *filename, DBT *key, DBT *data, void *userptr), void *userptr)
{
    struct log_queue_file *file = add_log_queue(filename, gethndl, func, userptr);
    if (file) {
        Pthread_create(&file->consumer, NULL, log_queue_consumer, file);
        log_trigger_register(filename, log_queue_trigger_callback);
        return 0;
    }

    return -1;
}

#include <fsnapf.h>
static void dump_qtrigger(const char *filename, DBT *key, DBT *data, void *userptr)
{
    logmsg(LOGMSG_USER, "Processing record for %s\n", filename);
    logmsg(LOGMSG_USER, "Key: \n");
    fsnapf(stdout, key->data, key->size);
    logmsg(LOGMSG_USER, "\n");
    logmsg(LOGMSG_USER, "Data (size %d): \n", data->size);
    fsnapf(stdout, data->data, data->size);
    logmsg(LOGMSG_USER, "\n");
    fflush(stdout);
    fflush(stderr);
}

void register_dump_qtrigger(const char *filename, bdb_state_type *(*gethndl)(const char *q))
{
    register_logqueue_trigger(filename, gethndl, dump_qtrigger, NULL);
}
