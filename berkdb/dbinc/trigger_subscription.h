#ifndef TRIGGER_SUBSCRIPTION_H
#define TRIGGER_SUBSCRIPTION_H

#include <pthread.h>
#include <inttypes.h>
#include <build/db_lsn.h>

#define STABLE_TRIGGER 0x00000001
typedef int trigger_enqueue_callback_func(DB_LSN commit_lsn, uint32_t commit_gen, uint64_t genid);

struct __db_trigger_subscription {
	char *name;
	int was_open; /* 1 if recovery should change it back to open */
	int active;
	uint8_t status;
	uint32_t flags;
	pthread_cond_t cond;
	pthread_mutex_t lock;
};

struct __db_trigger_subscription *__db_get_trigger_subscription(const char *);
int __db_for_each_trigger_subscription(hashforfunc_t *, int);
void __db_trigger_set_stable(const char *);
void __db_trigger_clear_stable(const char *);
int __db_trigger_enqueued_data(DB_LSN commit_lsn, uint32_t gen, uint64_t genid);
void __db_trigger_register_enqueue_callback(trigger_enqueue_callback_func cb);

#endif //TRIGGER_SUBSCRIPTION_H
