#ifndef STABLE_QUEUE_TRIGGER_H
#define STABLE_QUEUE_TRIGGER_H

/* Set gethndl function */
void register_gethndl(bdb_state_type *(*ingethndl)(const char *q));

/* Register as a stable queue */
void *register_stable_queue(const char *filename);

#endif
