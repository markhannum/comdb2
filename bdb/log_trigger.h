#ifndef LOG_TRIGGER_H
#define LOG_TRIGGER_H

#include <build/db.h>

int log_trigger_add_table(char *tablename);

void *log_trigger_btree_trigger(const char *btree);

int log_trigger_is_trigger(const char *table);

int log_trigger_callback(void *, const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename, u_int32_t rectype,
                         const void *log);

int log_trigger_register(const char *tablename, int (*cb)(const DB_LSN *lsn, const DB_LSN *commit_lsn,
                                                          const char *filename, u_int32_t rectype, const void *log));

#endif
