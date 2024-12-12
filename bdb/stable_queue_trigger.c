#include <log_trigger.h>
#include <cdb2_constants.h>
#include <bdb_int.h>
#include <assert.h>
#include <db_int.h>
#include <dbinc/btree.h>
#include <build/db.h>
#include <dbinc_auto/btree_auto.h>

#define MAX_STABLE_QUEUES 256

bdb_state_type *(*gethndl)(const char *q) = NULL;

struct stable_queue_state {
    char *filename;
    bdb_state_type *bdb_state;
    int usercnt;
};

static pthread_mutex_t stable_queue_lk = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t stable_queue_cd = PTHREAD_COND_INITIALIZER;
static int current_stable_queues = 0;
struct stable_queue_state stable_queues[MAX_STABLE_QUEUES] = {0};

static inline struct stable_queue_state *retrieve_stable_queue(const char *filename)
{
    Pthread_mutex_lock(&stable_queue_lk);
    for (int i = 0; i < current_stable_queues; i++) {
        if (stable_queues[i].filename && strcmp(stable_queues[i].filename, filename) == 0) {
            stable_queues[i].usercnt++;
            Pthread_mutex_unlock(&stable_queue_lk);
            return &stable_queues[i];
        }
    }
    Pthread_mutex_unlock(&stable_queue_lk);
    return NULL;
}

static inline void remove_stable_queue(const char *filename)
{
    Pthread_mutex_lock(&stable_queue_lk);
    for (int i = 0; i < current_stable_queues; i++) {
        if (strcmp(stable_queues[i].filename, filename) == 0) {
            while (stable_queues[i].usercnt > 0) {
                Pthread_cond_wait(&stable_queue_cd, &stable_queue_lk);
            }
            free(stable_queues[i].filename);
            stable_queues[i].filename = NULL;
            stable_queues[i].bdb_state = NULL;
        }
    }
    Pthread_mutex_unlock(&stable_queue_lk);
}

static inline struct stable_queue_state *add_stable_queue(const char *filename)
{
    int first_empty = -1;
    Pthread_mutex_lock(&stable_queue_lk);
    for (int i = 0; i < current_stable_queues; i++) {
        if (!stable_queues[i].filename && first_empty == -1) {
            first_empty = i;
        }
        if (strcmp(stable_queues[i].filename, filename) == 0) {
            stable_queues[i].usercnt++;
            Pthread_mutex_unlock(&stable_queue_lk);
            return &stable_queues[i];
        }
    }

    if (first_empty == -1 && current_stable_queues >= MAX_STABLE_QUEUES) {
        Pthread_mutex_unlock(&stable_queue_lk);
        logmsg(LOGMSG_ERROR, "%s: No additional slots for stable-queues\n", __func__);
        return NULL;
    }
    if (first_empty == -1) {
        first_empty = current_stable_queues++;
    }

    stable_queues[first_empty].filename = strdup(filename);
    stable_queues[first_empty].bdb_state = NULL;
    stable_queues[first_empty].usercnt = 1;

    Pthread_mutex_unlock(&stable_queue_lk);
    return &stable_queues[first_empty];
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

/* Expose this as api */
void register_gethndl(bdb_state_type *(*ingethndl)(const char *q))
{
    gethndl = ingethndl;
}

static int stable_queue_write(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *infilename, u_int32_t rectype,
                              const void *log)
{
    /* Immediately filter records we won't use */
    normalize_rectype(&rectype);
    if (rectype > 1000 && rectype < 10000) {
        rectype = rectype - 1000;
    }

    if (rectype != DB___db_addrem) {
        return 0;
    }

    const __db_addrem_args *argp = log;
    if (argp->opcode != DB_ADD_DUP) {
        return 0;
    }

    if (argp->indx % 2 != 0) {
        return 0;
    }

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

    struct stable_queue_state *sq = retrieve_stable_queue(filename);
    if (!sq) {
        return -1;
    }

    /* Key-size must be 12 */
    assert(argp->dbt.size == 12);

    u_int64_t genid;
    memcpy(&genid, &((u_int32_t *)argp->dbt.data)[1], 8);

    if (sq->bdb_state == NULL && gethndl != NULL) {
        sq->bdb_state = gethndl(filename);
    }

    if (sq->bdb_state != NULL) {
        /* Log-triggers are called inline with replication while a transaction's locks
         * are held.  This prevents anything from being viewed while we update the
         * stable-queue list */
        update_stable_queue(sq->bdb_state, genid, commit_lsn);
    }
    Pthread_mutex_lock(&stable_queue_lk);
    sq->usercnt--;
    assert(sq->usercnt >= 0);
    Pthread_cond_signal(&stable_queue_cd);
    Pthread_mutex_unlock(&stable_queue_lk);

    return 0;
}

void *register_stable_queue(const char *filename)
{
    struct stable_queue_state *sq = add_stable_queue(filename);
    if (sq) {
        log_trigger_register(filename, stable_queue_write, 0);
        return sq;
    }
    return NULL;
}
