#ifndef LOG_QUEUE_TRIGGER_H
#define LOG_QUEUE_TRIGGER_H

/* Log-trigger which rebuilds records on a queue
 * starting here, but this might move to bb-plugins */
int log_queue_trigger_callback(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename, int op,
                               const void *log);

#endif
