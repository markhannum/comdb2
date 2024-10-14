/*
 * Rebuild queue-payload from replicant's transaction log
 */
#include <log_trigger.h>
#include <bdb_int.h>
#include <assert.h>
#include <db_int.h>
#include <dbinc/btree.h>
#include <build/db.h>
#include <dbinc_auto/btree_auto.h>

#define MAX_QUEUE_FILES 64

#define FLAGS_BIG 1

static comdb2ma logqma;

struct log_queue_transaction {
    u_int64_t txnid;
    u_int8_t key[12];
    u_int8_t *record;
    void *freeptr;
    struct odh odh;
    int record_len;
    int offset;
    u_int32_t flags;
    LINKC_T(struct log_queue_transaction) lnk;
};

struct log_queue_file {
    char *filename;
    bdb_state_type *bdb_state;
    pthread_mutex_t lk;
    pthread_cond_t cond;
    hash_t *transactions;
    LISTC_T(struct log_queue_transaction) outqueue;
    pthread_t consumer;
};

static int current_queue_files = 0;
struct log_queue_file log_queue_files[MAX_QUEUE_FILES] = {0};

static void *qmalloc(size_t sz)
{
    return comdb2_malloc(logqma, sz);
}

static void qfree(void *ptr)
{
    comdb2_free(ptr);
}

/* File lookup intended to be mostly lockless */
static inline struct log_queue_file *retrieve_log_queue(const char *filename)
{
    for (int i = 0; i < current_queue_files; i++) {
        if (strcmp(log_queue_files[i].filename, filename) == 0) {
            return &log_queue_files[i];
        }
    }
    return NULL;
}

static struct log_queue_file *add_log_queue(const char *filename, bdb_state_type *bdb_state)
{
    struct log_queue_file *file = retrieve_log_queue(filename);
    if (file) {
        file->bdb_state = bdb_state;
        return file;
    }
    if (current_queue_files >= MAX_QUEUE_FILES) {
        logmsg(LOGMSG_ERROR, "%s out of queue-files\n", __func__);
        return NULL;
    }

    log_queue_files[current_queue_files].filename = comdb2_strdup(logqma, filename);
    log_queue_files[current_queue_files].transactions = hash_setalloc_init(qmalloc, qfree, 0, sizeof(u_int64_t));
    log_queue_files[current_queue_files].bdb_state = bdb_state;
    listc_init(&log_queue_files[current_queue_files].outqueue, offsetof(struct log_queue_transaction, lnk));

    Pthread_mutex_init(&log_queue_files[current_queue_files].lk, NULL);
    Pthread_cond_init(&log_queue_files[current_queue_files].cond, NULL);
    current_queue_files++;
    return &log_queue_files[current_queue_files - 1];
}

extern int bdb_unpack_updateid(bdb_state_type *bdb_state, const void *from, size_t fromlen, void *to, size_t tolen,
                               struct odh *odh, int updateid, void **freeptr, int verify_updateid, int force_odh,
                               void *(*fn_malloc)(size_t), void (*fn_free)(void *));

static int unpack_and_queue(struct log_queue_file *file, struct log_queue_transaction *txn)
{
    bdb_unpack_updateid(file->bdb_state, txn->record, txn->record_len, NULL, 0, &txn->odh, 0, &txn->freeptr, 0, 0,
                        qmalloc, qfree);
    Pthread_mutex_lock(&file->lk);
    listc_abl(&file->outqueue, txn);
    Pthread_cond_signal(&file->cond);
    Pthread_mutex_unlock(&file->lk);
    return 0;
}

static int handle_big(struct log_queue_file *file, u_int64_t txnid, const __db_big_args *argp)
{
    Pthread_mutex_lock(&file->lk);
    struct log_queue_transaction *txn = hash_find(file->transactions, &txnid);
    assert(txn);
    txn->flags |= FLAGS_BIG;
    txn->record = comdb2_realloc(logqma, txn->record, txn->record_len + argp->dbt.size);
    memcpy(txn->record + txn->record_len, argp->dbt.data, argp->dbt.size);
    txn->record_len += argp->dbt.size;
    Pthread_mutex_unlock(&file->lk);
    return 0;
}

static int handle_addrem(struct log_queue_file *file, u_int64_t txnid, const __db_addrem_args *argp)
{
    Pthread_mutex_lock(&file->lk);
    struct log_queue_transaction *txn = hash_find(file->transactions, &txnid);
    if (txn == NULL) {

        assert(argp->indx % 2 == 0);
        assert(argp->dbt.size == 12);

        txn = comdb2_calloc(logqma, sizeof(struct log_queue_transaction), 1);
        txn->txnid = txnid;
        memcpy(txn->key, argp->dbt.data, 12);
        hash_add(file->transactions, txn);
        Pthread_mutex_unlock(&file->lk);
        return 0;
    }

    // Either short-data case or we've finished collecting overflow records
    // and are updating original leaf-page
    assert(argp->indx % 2 == 1);
    if (!(txn->flags & FLAGS_BIG)) {
        txn->record = comdb2_malloc(logqma, argp->dbt.size);
        memcpy(txn->record, argp->dbt.data, argp->dbt.size);
    }
    hash_del(file->transactions, txn);
    Pthread_mutex_unlock(&file->lk);
    return unpack_and_queue(file, txn);
}

static int log_queue_trigger_callback(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename,
                                      u_int32_t rectype, const void *log)
{
    struct log_queue_file *file = retrieve_log_queue(filename);
    if (file == NULL) {
        return -1;
    }

    int rc = normalize_rectype(&rectype);

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

int log_queue_trigger_init(void)
{
    logqma = comdb2ma_create(0, 0, "log_queue_triggers", 0);
    return 0;
}

void *log_queue_consumer(void *arg)
{
    struct log_queue_file *file = arg;
    while (1) {
        Pthread_mutex_lock(&file->lk);
        while (listc_size(&file->outqueue) == 0) {
            Pthread_cond_wait(&file->cond, &file->lk);
        }
        struct log_queue_transaction *txn = listc_rtl(&file->outqueue);
        logmsg(LOGMSG_USER, "Consuming txnid %lu\n", txn->txnid);
        Pthread_mutex_unlock(&file->lk);
    }
}

/* TODO: maybe register another callback to invoke from here
 * we will call the callback with the packed record
 * and free what we don't use. */
int register_logqueue_trigger(bdb_state_type *bdb_state, const char *filename)
{
    struct log_queue_file *file = add_log_queue(filename, bdb_state);
    if (file) {
        Pthread_create(&file->consumer, NULL, log_queue_consumer, file);
        log_trigger_register(filename, log_queue_trigger_callback);
        return 0;
    }

    return -1;
}
