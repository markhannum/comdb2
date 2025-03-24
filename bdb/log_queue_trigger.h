#ifndef LOG_QUEUE_TRIGGER_H
#define LOG_QUEUE_TRIGGER_H
#include <bdb_api.h>
#include <bdb_int.h>

/* Log-queue-triggers are for physical-replicants (at least cut-1).  Because
 * these follow the same codepath as normal-replicants, we do not have to
 * write custom master-side code */

enum {
    LOGQTRIGGER_ENABLED = 1,
    LOGQTRIGGER_PUSH = 2,
    LOGQTRIGGER_PULL = 4,
    LOGQTRIGGER_MASTER_ONLY = 8,
};

typedef void (*logqueue_trigger_callback)(bdb_state_type *bdb_state, const DB_LSN *commit_lsn, const char *filename,
                                          const DBT *key, const DBT *data, void *userptr);

/* Log-trigger invokes callback when a record is written to a queue in push-mode */
void *register_logqueue_trigger(const char *filename, bdb_state_type *(*gethndl)(const char *q),
                                logqueue_trigger_callback func, void *userptr, size_t maxsz, uint32_t flags);

/* Pull mode removes record from front of queue */
int logqueue_trigger_get(bdb_state_type *bdb_state, const struct bdb_queue_cursor *prevcursor,
                         struct bdb_queue_found **fnd, size_t *fnddtalen, size_t *fnddtaoff,
                         struct bdb_queue_cursor *fndcursor, long long *seq, int *bdberr, int timeoutms);

/* Consume this genid from the memory-queue */
int logqueue_trigger_consume(bdb_state_type *bdb_state, uint64_t genid);

/* Enable log-queue-trigger on consumer-subscribe */
void logqueue_trigger_enable(void *infile);

/* Disable log-queue-trigger on consumer-unsubscribe */
void logqueue_trigger_disable(void *infile);

/* Register this queue as a memory-queue */
void register_memq(const char *filename, bdb_state_type *(*gethndl)(const char *q), int maxsz);

/* Return subscription object for queue, or NULL if it is not a memory-queue */
void *queue_is_memq(const char *filename);

/* Test function */
void register_dump_qtrigger(const char *filename, bdb_state_type *(*gethndl)(const char *q), const char *outfile,
                            int maxsz);

#ifdef WITH_QKAFKA

int register_queue_kafka(const char *filename, const char *kafka_topic, bdb_state_type *(*gethndl)(const char *q),
                         int maxsz);

#endif

#endif
