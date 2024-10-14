#ifndef LOG_QUEUE_TRIGGER_H
#define LOG_QUEUE_TRIGGER_H
#include <bdb_int.h>

/* Log-trigger which rebuilds records on a queue
 * starting here, but this might move to bb-plugins */

/* TODO: fix/normalize names */

int register_logqueue_trigger(const char *filename,
        bdb_state_type *(*gethndl)(const char *q),
        void (*func)(const char *filename, DBT *key, DBT *data, void *userptr),
        void *userptr);

/* Test function */
void register_dump_qtrigger(const char *filename, bdb_state_type *(*gethndl)(const char *q));

/* Clear all queues from recovery */
void clear_log_queues(void);

#endif
