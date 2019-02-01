#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>
#include <inttypes.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include <sqlite3.h>

#include <cdb2api.h>
#include <plhash.h>

#include "passfd.h"
#include "comdb2buf.h"
#include "strbuf.h"

#define MAXHOSTS 64

struct db_hosts {
    int nhosts;
    int sameroom;
    char** hosts;
    char** rooms;
};

struct db_name {
    char *name;
    char *stage;
};

struct db_destination {
    time_t last_accessed;
    int64_t resolve_requests, passthrough_requests;
    struct db_name dbname;
    struct db_hosts discovered_hosts;
    struct db_hosts fallback_hosts;
};

static pthread_mutex_t destlk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *cached_destinations = NULL;

int resolve_from_comdb2db = 1;

unsigned int hashf(const void *key, int len) {
    struct db_name *dbname = (struct db_name*) key;
    unsigned u = hash_default_fixedwidth(dbname->name, strlen(dbname->name)) ^
           hash_default_fixedwidth(dbname->stage, strlen(dbname->stage));
}

int cmpf(const void *key1, const void *key2, int len) {
    struct db_name *dbname1 = (struct db_name*) key1,
                   *dbname2 = (struct db_name*) key2;
    int cmp = strcmp(dbname1->name, dbname2->name) || strcmp(dbname1->stage, dbname2->stage);
    return cmp;
}

static char *myroom = "ORG";

sqlite3 *fallback_db;

void save_to_fallback(char *dbname, char *stage, struct db_hosts *dest, int replace);
struct db_hosts *discover_from_comdb2db(const char *dbname, const char *stage);

int passthrough_allowed = 1;
int emergency_passthrough = 0;
int always_passthrough = 0;

int64_t comdb2db_errors = 0;
int64_t comdb2db_ok = 0;
int error_threshold = 10;
int recover_threshold = 20;
time_t last_error = 0;

void init_fallback_db(void) {
    int rc;
    char *err = NULL;
    char host[256];

    rc = sqlite3_exec(fallback_db, "create table databases(dbname text, stage text, host text, room text);", NULL, NULL, NULL);
    if (rc) 
        goto bad;
    rc = sqlite3_exec(fallback_db, "create index databases_ix0 on databases(dbname, stage)", NULL, NULL, NULL);
    if (rc)
        goto bad;

    return;

bad:
    fprintf(stderr, "init_fallback_db: %s\n", err ? err : "???");
    sqlite3_close(fallback_db);
    fallback_db = NULL;
}

void load_fallback(void) {
    int rc;
    sqlite3_stmt *stmt = NULL;
    struct db_destination *dest = NULL;

    rc = sqlite3_prepare(fallback_db, "select dbname, stage, host, room from databases order by dbname, (room = @room),  stage;", -1, &stmt, NULL);
    if (rc == 0)
        rc = sqlite3_bind_text(stmt, 1, myroom, -1, SQLITE_STATIC);
    if (rc) {
        printf("prepare rc %d %s\n", rc, sqlite3_errmsg(fallback_db));
        printf("recreating fallback db\n");
        init_fallback_db();
        return;
    }
    char *prevname = "~~~~~~~";
    rc = sqlite3_step(stmt);
    while (rc == SQLITE_ROW) {
        const char *dbname, *stage, *host, *room;
        dbname = sqlite3_column_text(stmt, 0);
        stage  = sqlite3_column_text(stmt, 1);
        host  = sqlite3_column_text(stmt, 2);
        room  = sqlite3_column_text(stmt, 3);

        if (strcmp(dbname, prevname)) {
            printf("loaded from fallback: %s/%s\n", dbname, stage);
            if (dest != NULL) {
                hash_add(cached_destinations, dest);
            }

            dest = malloc(sizeof(struct db_destination));
            dest->dbname.name = strdup(dbname);
            dest->dbname.stage = strdup(stage);

            dest->discovered_hosts.nhosts = 0;
            dest->discovered_hosts.sameroom = 0;
            dest->discovered_hosts.hosts = NULL;
            dest->discovered_hosts.rooms = NULL;

            dest->fallback_hosts.nhosts = 0;
            dest->fallback_hosts.sameroom = 0;
            dest->fallback_hosts.hosts = NULL;
            dest->fallback_hosts.rooms = NULL;

            dest->resolve_requests = 0;
            dest->passthrough_requests = 0;
        }

        dest->fallback_hosts.hosts = realloc(dest->fallback_hosts.hosts, sizeof(char*) * (dest->fallback_hosts.nhosts+1));
        dest->fallback_hosts.rooms = realloc(dest->fallback_hosts.rooms, sizeof(char*) * (dest->fallback_hosts.nhosts+1));
        dest->fallback_hosts.hosts[dest->fallback_hosts.nhosts] = strdup(host);
        if (strcmp(room, myroom) == 0)
            dest->fallback_hosts.sameroom++;
        dest->fallback_hosts.rooms[dest->fallback_hosts.nhosts] = strdup(room);
        dest->fallback_hosts.nhosts++;
        dest->last_accessed = time(NULL);
        prevname = dest->dbname.name;

        rc = sqlite3_step(stmt);
    }
    if (dest != NULL) {
        hash_add(cached_destinations, dest);
    }

    if (rc != SQLITE_DONE) {
        printf("run rc %d %s\n", rc, sqlite3_errmsg(fallback_db));
    }
    if (stmt)
        sqlite3_finalize(stmt);
}

struct destcpy {
    int ix;
    struct db_destination *dests;
};


struct db_destination clonedest(struct db_destination *in) {
    struct db_destination dest;
    dest.last_accessed = in->last_accessed;
    dest.dbname.name = strdup(in->dbname.name);
    dest.dbname.stage = strdup(in->dbname.stage);
    dest.discovered_hosts.nhosts = in->discovered_hosts.nhosts;
    dest.discovered_hosts.sameroom = in->discovered_hosts.sameroom;
    dest.discovered_hosts.hosts = malloc(sizeof(char*) * dest.discovered_hosts.nhosts);
    dest.discovered_hosts.rooms = malloc(sizeof(char*) * dest.discovered_hosts.nhosts);
    for (int i = 0; i < dest.discovered_hosts.nhosts; i++) {
        dest.discovered_hosts.hosts[i] = strdup(in->discovered_hosts.hosts[i]);
        dest.discovered_hosts.rooms[i] = strdup(in->discovered_hosts.rooms[i]);
    }

    dest.fallback_hosts.nhosts = in->fallback_hosts.nhosts;
    dest.fallback_hosts.sameroom = in->fallback_hosts.sameroom;
    dest.fallback_hosts.hosts = malloc(sizeof(char*) * dest.fallback_hosts.nhosts);
    dest.fallback_hosts.rooms = malloc(sizeof(char*) * dest.fallback_hosts.nhosts);
    for (int i = 0; i < dest.fallback_hosts.nhosts; i++) {
        dest.fallback_hosts.hosts[i] = strdup(in->fallback_hosts.hosts[i]);
        dest.fallback_hosts.rooms[i] = strdup(in->fallback_hosts.rooms[i]);
    }

    return dest;
}

int copydest(void *obj, void *arg) {
    struct db_destination *dest = (struct db_destination*) obj;
    struct destcpy *copy = (struct destcpy*) arg;
    copy->dests[copy->ix++] = clonedest(dest);
    return 0;
}

#if 0
       void qsort(void *base, size_t nmemb, size_t size,
                  int(*compar)(const void *, const void *));
#endif

int cmpdests(const void *dp1, const void *dp2) {
    struct db_destination *d1 = (struct db_destination*) dp1;
    struct db_destination *d2 = (struct db_destination*) dp2;
}

void free_dest(struct db_destination *dest) {
    free(dest->dbname.name);
    free(dest->dbname.stage);
    for (int i = 0; i < dest->discovered_hosts.nhosts; i++) {
        free(dest->discovered_hosts.hosts[i]);
        free(dest->discovered_hosts.rooms[i]);
    }
    for (int i = 0; i < dest->fallback_hosts.nhosts; i++) {
        free(dest->fallback_hosts.hosts[i]);
        free(dest->fallback_hosts.rooms[i]);
    }
}

int hosts_changed(const struct db_hosts *h1, const struct db_hosts *h2) {
    if (h1->nhosts != h2->nhosts)
        return 1;
    if (h1->sameroom != h2->sameroom)
        return 1;
    for (int i = 0; i < h1->nhosts; i++) {
        if (strcmp(h1->hosts[i], h2->hosts[i]))
            return 1;
        if (strcmp(h1->rooms[i], h2->rooms[i]))
            return 1;
    }

    return 0;
}

enum hostlist_type {
    HOSTS_DISCOVERED,
    HOSTS_FALLBACK
};

int update_hosts(const char *dbname, const char *stage, struct db_hosts *newdest, enum hostlist_type which) {
    pthread_mutex_lock(&destlk);
    struct db_destination *dest;
    struct db_name name;

    name.name = (char*) dbname;
    name.stage = (char*) stage;

    dest = hash_find(cached_destinations, &name);
    if (dest == NULL)
        fprintf(stderr, "asked to check db, but no longer in list: \"%s/%s\"\n", dbname, stage);
    else {
        struct db_hosts *target;
        switch (which) {
            case HOSTS_DISCOVERED:
                target = &dest->discovered_hosts;
                break;
            case HOSTS_FALLBACK:
                target = &dest->fallback_hosts;
                break;

            default:
                pthread_mutex_unlock(&destlk);
                return -1;
        }

        // free old value if any
        if (target->nhosts) {
            for (int i = 0; i < target->nhosts; i++) {
                free(target->hosts[i]);
                free(target->rooms[i]);
            }
        }


        target->nhosts = newdest->nhosts;
        target->sameroom = newdest->sameroom;
        target->hosts = malloc(sizeof(char*) * newdest->nhosts);
        target->rooms = malloc(sizeof(char*) * newdest->nhosts);
        for (int i = 0; i < newdest->nhosts; i++) {
            target->hosts[i] = strdup(newdest->hosts[i]);
            target->rooms[i] = strdup(newdest->rooms[i]);
        }
    }
    pthread_mutex_unlock(&destlk);
}

static pthread_mutex_t rechecklk = PTHREAD_MUTEX_INITIALIZER;

int recheck(void) {
    struct destcpy copy = {0};
    int nent = 0;
    char *dbname, *stage;

    pthread_mutex_lock(&rechecklk);

    // make a copy of the destinations - we don't want to hold a lock while doing database queries
    pthread_mutex_lock(&destlk);
    hash_info(cached_destinations, NULL, NULL, NULL, NULL, &nent, NULL, NULL);
    copy.dests = malloc(nent * sizeof(struct db_destination));
    hash_for(cached_destinations, copydest, &copy);
    pthread_mutex_unlock(&destlk);

    // now re-check/persist without holding lock
    for (int i = 0; i < copy.ix; i++) {
        dbname = copy.dests[i].dbname.name;
        stage = copy.dests[i].dbname.stage;

        printf("recheck: %s/%s  ->  ", copy.dests[i].dbname.name, copy.dests[i].dbname.stage);

        // if we newly discovered this database, save it to fallback
        if (copy.dests[i].fallback_hosts.nhosts == 0 && copy.dests[i].discovered_hosts.nhosts > 0) {
            printf("new db: %s/%s, persisting to fallback\n", copy.dests[i].dbname.name, copy.dests[i].dbname.stage);
            save_to_fallback(dbname, stage, &copy.dests[i].discovered_hosts, 1);

            // and update the cached copy, like it would be when we read it from startup
            update_hosts(dbname, stage, &copy.dests[i].discovered_hosts, HOSTS_FALLBACK);
        }
        else {
            // requery the db
            struct db_hosts *newhosts = discover_from_comdb2db(dbname, stage);
            if (newhosts == NULL)
                printf("can't resolve %s/%s\n", dbname, stage);
            else if (hosts_changed(newhosts, &copy.dests[i].discovered_hosts)) {
                printf("hosts changed: %s/%s\n", copy.dests[i].dbname.name, copy.dests[i].dbname.stage);
                if (hosts_changed(newhosts, &copy.dests[i].fallback_hosts)) {
                    update_hosts(dbname, stage, newhosts, HOSTS_FALLBACK);
                    save_to_fallback(dbname, stage, newhosts, 1);
                }
                update_hosts(dbname, stage, newhosts, HOSTS_DISCOVERED);
            }
            else {
                printf("no change\n");
            }
            if (newhosts) {
                free(newhosts->hosts);
                free(newhosts->rooms);
                free(newhosts);
            }
        }
    }
    for (int i = 0; i < copy.ix; i++) {
        free_dest(&copy.dests[i]);
    }

    pthread_mutex_unlock(&rechecklk);
}


void* recheck_thread(void* unused) {
    for (;;) {
        recheck();
        sleep(30);
    }
}

void run_recheck_thread(void) {
    pthread_t tid;
    pthread_create(&tid, NULL, recheck_thread, NULL);
}

void cached_destinations_init(void) {
    cached_destinations = hash_init_user(hashf, cmpf, offsetof(struct db_destination, dbname), sizeof(struct db_name));
    int fd=-1;


    // try to figure out our room
    // TODO: also comdb2dbname/tier
    // TODO: also sockpool fallback path?
    fd = open("/opt/bb/etc/cdb2/config/comdb2db.cfg", O_RDONLY);
    if (fd == -1) {
        myroom = "";
        return;
    }
    COMDB2BUF *buf = cdb2buf_open(fd, 0);
    char line[256];
    while (cdb2buf_gets(line, sizeof(line), buf)) {
        char *tok;
        tok = strchr(line, '\n');
        if (tok)
            *tok = 0;
        tok = strtok(line, " :");
        if (tok == NULL)
            continue;
        if (strcasecmp(tok, "comdb2_config") == 0) {
            tok = strtok(NULL, " =:,");
            if (tok == NULL)
                continue;
            if (strcasecmp(tok, "room") == 0) {
                tok = strtok(NULL, " :,");
                if (tok) {
                    myroom = strdup(tok);
                    break;
                }
            }
        }
    }
    cdb2buf_close(buf);

    int rc = sqlite3_open("/bb/data/sockpool.fallback", &fallback_db);
    if (rc == 0)
        load_fallback();
    else {
        int rc = sqlite3_open("/tmp/sockpool.fallback", &fallback_db);
        if (rc == 0)
            load_fallback();
        else
            fprintf(stderr, "No fallback db available, open rc %d\n", rc);
    }

    recheck();
    run_recheck_thread();
}

void save_to_fallback(char *dbname, char *stage, struct db_hosts *dest, int replace) {
    sqlite3_stmt *stmt = NULL;
    int rc;
    int havetran = 1;

    if (fallback_db == NULL)
        return;

    rc = sqlite3_exec(fallback_db, "begin", NULL, NULL, NULL);
    if (rc)
        goto bad;
    havetran = 1;

    if (replace) {
        rc = sqlite3_prepare(fallback_db, "delete from databases where dbname = @dbname and stage = @stage", -1, &stmt, NULL);
        if (rc)
            goto bad;
        rc = sqlite3_bind_text(stmt, 1, dbname, -1, SQLITE_STATIC);
        if (rc)
            goto bad;
        rc = sqlite3_bind_text(stmt, 2, stage, -1, SQLITE_STATIC);
        if (rc)
            goto bad;
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE)
            goto bad;
        rc = sqlite3_reset(stmt);
        stmt = NULL;
        if (rc)
            goto bad;
    }

    rc = sqlite3_prepare(fallback_db, "insert into databases(dbname, stage, host, room) values(@dbname, @stage, @host, @room)", -1, &stmt, NULL);
    if (rc) {
        fprintf(stderr, "%s: prepare rc %d %s\n", rc, sqlite3_errmsg(fallback_db));
        return;
    }
    for (int i = 0; i < dest->nhosts; i++) {
        rc = sqlite3_bind_text(stmt, 1, dbname, -1, SQLITE_STATIC);
        if (rc) goto bad;
        rc = sqlite3_bind_text(stmt, 2, stage, -1, SQLITE_STATIC);
        if (rc) goto bad;
        rc = sqlite3_bind_text(stmt, 3, dest->hosts[i], -1, SQLITE_STATIC);
        if (rc) goto bad;
        rc = sqlite3_bind_text(stmt, 4, dest->rooms[i], -1, SQLITE_STATIC);
        if (rc) goto bad;
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_DONE)
            goto bad;
        rc = sqlite3_reset(stmt);
        if (rc)
            goto bad;
    }
    rc = sqlite3_exec(fallback_db, "commit", NULL, NULL, NULL);
    if (rc)
        goto bad;
    sqlite3_finalize(stmt);
    stmt = NULL;
    return;

bad:
    fprintf(stderr, "failed to write fallback information for %s %s: %d %s\n", dbname, stage, rc, sqlite3_errmsg(fallback_db));
    if (stmt)
        sqlite3_finalize(stmt);
    if (havetran)
        sqlite3_exec(fallback_db, "rollback", NULL, NULL, NULL);
}

struct db_hosts *discover_from_comdb2db(const char *dbname, const char *stage) {
    struct db_hosts *dest;
    char *hosts[MAXHOSTS];
    char *rooms[MAXHOSTS];
    int nhosts = 0;
    int sameroom = 0;

    if (!resolve_from_comdb2db) {
        comdb2db_errors++;
        last_error = time(NULL);
        return NULL;
    }

    cdb2_hndl_tp *db;
    int rc = cdb2_open(&db, "comdb3db", "dev", 0);
    if (rc)
        return NULL;
    cdb2_bind_param(db, "dbname", CDB2_CSTRING, dbname, strlen(dbname));
    cdb2_bind_param(db, "cluster", CDB2_CSTRING, stage, strlen(dbname));
    cdb2_bind_param(db, "room", CDB2_CSTRING, myroom, strlen(dbname));
    rc = cdb2_run_statement(db, "select M.name, M.room from machines M join databases D "
                            "where M.cluster IN (select cluster_machs from clusters where "
                            "name=@dbname and cluster_name=@cluster) and D.name=@dbname "
                            "order by (room = @room) desc, M.name, M.room");
    if (rc) {
        comdb2db_errors++;
        last_error = time(NULL);
        cdb2_close(db);
        return NULL;
    }
    rc = cdb2_next_record(db);
    while (rc == CDB2_OK) {
        hosts[nhosts] = strdup(cdb2_column_value(db, 0));
        rooms[nhosts++] = strdup(cdb2_column_value(db, 1));
        char *room = cdb2_column_value(db, 1);
        if (strcmp(room, myroom) == 0)
            sameroom++;
        rc = cdb2_next_record(db);
    }
    cdb2_close(db);
    if (rc != CDB2_OK_DONE) {
        last_error = time(NULL);
        comdb2db_errors++;
        for (int i = 0; i < nhosts; i++) {
            free(hosts[i]);
        }
        return NULL;
    }
    dest = malloc(sizeof(struct db_hosts));

    dest->nhosts = nhosts;
    dest->sameroom = sameroom;
    dest->hosts = malloc(sizeof(char*) * nhosts);
    dest->rooms = malloc(sizeof(char*) * nhosts);
    for (int i = 0; i < nhosts; i++) {
        dest->hosts[i] = hosts[i];
        dest->rooms[i] = rooms[i];
    }

    comdb2db_ok++;

    return dest;
}

struct sockpool_resolve_response {
    int nhosts;
    char hostname[128][64];
    int num_same_room;
};

/* Returns a found destination or NULL if it's not cached and can't be found
 * in comdb2db.  Must be called with with destlk held. WARNING: may release/reacquire
 * destlk mid-call.  */
static struct db_destination *resolve(const char *dbname, const char *stage) {
    struct db_name name;
    struct db_destination *dest;
    time_t now = time(NULL);

    name.name = (char*) dbname;
    name.stage = (char*) stage;

    dest = hash_find(cached_destinations, &name);
    if (!dest) {
        pthread_mutex_unlock(&destlk);
        struct db_hosts *hosts = discover_from_comdb2db(dbname, stage);
        if (hosts == NULL) {
            fprintf(stderr, "can't resolve \"%s/%s\n", dbname, stage);
            pthread_mutex_lock(&destlk);
            goto done;
        }
        dest = malloc(sizeof(struct db_destination));
        dest->dbname.name = strdup(dbname);
        dest->dbname.stage = strdup(stage);
        dest->discovered_hosts = *hosts;
        dest->resolve_requests = 0;
        dest->passthrough_requests = 0;

        // let the checker thread populate this later
        dest->fallback_hosts.nhosts = 0;
        free (hosts);

        pthread_mutex_lock(&destlk);
        struct db_destination *chkdest = hash_find(cached_destinations, &name);

        if (! chkdest) {
            printf("adding new dest: %s/%s\n", dbname, stage);
            hash_add(cached_destinations, dest);
        }
        else {
            free_dest(dest);
            free(dest);
            dest = chkdest;
        }
    }
    dest->last_accessed = now;
    dest->resolve_requests++;

done:
    return dest;
}

int discover(int fd, char *request) {
    char *s = strdup(request);
    char *dbname = s;
    char *stage = strchr(s, '/');
    int rc;

    if (stage) {
        *stage = 0;
        stage++;
    }
    else {
        /* TODO - return error? */
    }
    struct sockpool_resolve_response rsp = {0};
    struct db_destination *dest;

    pthread_mutex_lock(&destlk);
    dest = resolve(dbname, stage);

    rsp.nhosts = 0;
    if (dest) {
        if (dest->discovered_hosts.nhosts > 0) {
            rsp.nhosts = dest->discovered_hosts.nhosts;
            for (int i = 0; i < rsp.nhosts; i++) {
                strcpy(rsp.hostname[i], dest->discovered_hosts.hosts[i]);
            }
            rsp.num_same_room = dest->discovered_hosts.sameroom;
            printf("returning from discovered\n");
        }
        else if (dest->fallback_hosts.nhosts > 0) {
            rsp.nhosts = dest->fallback_hosts.nhosts;
            for (int i = 0; i < rsp.nhosts; i++) {
                strcpy(rsp.hostname[i], dest->fallback_hosts.hosts[i]);
            }
            rsp.num_same_room = dest->fallback_hosts.sameroom;
            printf("returning from fallback\n");
        }
    }
    else {
        printf("can't resolve %s/%s\n", dbname, stage);
    }
    pthread_mutex_unlock(&destlk);
    free(s);

done:
    rc = send_fd(fd, &rsp, sizeof(rsp), -1);
    if (rc) {
        printf("send rc %d\n", rc);
        return -1;
    }

    return 0;
} 

int clear_dest(void *obj, void *user) {
    struct db_destination *dest = (struct db_destination*) obj;
    printf("clearing: %s/%s\n", dest->dbname.name, dest->dbname.stage);
    free_dest(dest);
    free(dest);
    return 0;
}

int dump_dest(void *obj, void *user) {
    struct db_destination *dest = (struct db_destination*) obj;
    printf("%s/%s %"PRId64" requests  %"PRId64" passthroughs\n", dest->dbname.name, dest->dbname.stage, dest->resolve_requests, dest->passthrough_requests);
    printf("   discovered: %d hosts: ", dest->discovered_hosts.nhosts);
    for (int i = 0; i < dest->discovered_hosts.nhosts; i++)
        printf("%s (%s)  ", dest->discovered_hosts.hosts[i], dest->discovered_hosts.rooms[i]);
    printf("\n");
    printf("   fallback: %d hosts: ", dest->fallback_hosts.nhosts);
    for (int i = 0; i < dest->fallback_hosts.nhosts; i++)
        printf("%s (%s)  ", dest->fallback_hosts.hosts[i], dest->fallback_hosts.rooms[i]);
    printf("\n");
    return 0;
}

void discover_msg(int ntoks, char *toks[]) {
    if (ntoks < 2)
        return;

    if (strcmp(toks[1], "clearall") == 0) {
        pthread_mutex_lock(&destlk);
        hash_for(cached_destinations, clear_dest, NULL);
        hash_clear(cached_destinations);
        pthread_mutex_unlock(&destlk);
    }
    else if (strcmp(toks[1], "clear") == 0) {
        pthread_mutex_lock(&destlk);
        hash_for(cached_destinations, clear_dest, NULL);
        hash_clear(cached_destinations);
        load_fallback();
        pthread_mutex_unlock(&destlk);
    }
    else if (strcmp(toks[1], "dump") == 0) {
        pthread_mutex_lock(&destlk);
        hash_for(cached_destinations, dump_dest, NULL);
        pthread_mutex_unlock(&destlk);
    }
    else if (strcmp(toks[1], "disable") == 0) {
        if (ntoks < 3)
            return;
        if (strcmp(toks[2], "comdb2db") == 0)
            resolve_from_comdb2db = 0;
        else if (strcmp(toks[2], "passthrough") == 0)
            passthrough_allowed = 0;
        else {
            fprintf(stderr, "Unknown option\n");
        }
    }
    else if (strcmp(toks[1], "enable") == 0) {
        if (ntoks < 3)
            return;
        if (strcmp(toks[2], "comdb2db") == 0)
            resolve_from_comdb2db = 1;
        else if (strcmp(toks[2], "passthrough") == 0)
            passthrough_allowed = 1;
        else {
            fprintf(stderr, "Unknown option\n");
        }
    }
    else if (strcmp(toks[1], "refresh") == 0) {
        recheck();
    }
}

int discover_passthrough(const char *typestr) {
    if (!passthrough_allowed)
        return -1;

    if (!(emergency_passthrough || always_passthrough))
        return -1;

    char *s = strdup(typestr);
    int fd = -1;
    struct db_destination *dest = NULL;
    strbuf *buf = NULL;

    /* comdb2/dbname/stage/newsql/policy */
    // printf("passthrough: %s\n", typestr);
    const char *dbname, *stage, *policy, *unused;
    char *e;
    unused = strtok_r(s, "/", &e);
    if (unused == NULL)
        goto done;
    dbname = strtok_r(NULL, "/", &e);
    if (dbname == NULL)
        goto done;
    stage = strtok_r(NULL, "/", &e);
    if (stage == NULL)
        goto done;
    unused = strtok_r(NULL, "/", &e);
    if (unused == NULL)
        goto done;
    policy = strtok_r(NULL, "/", &e);
    if (policy == NULL)
        goto done;

    // printf("dbname %s stage %s policy %s\n", dbname, stage, policy);

    /* @machine:port=123:dc=ZONE1,machine2:port=456:dc=ZONE2 */
    pthread_mutex_lock(&destlk);
    dest = resolve(dbname, stage);
    if (dest == NULL) {
        pthread_mutex_unlock(&destlk);
        goto done;
    }
    struct db_hosts *h;
    if (dest->discovered_hosts.nhosts > 0)
        h = &dest->discovered_hosts;
    else
        h = &dest->fallback_hosts;
    dest->resolve_requests--;     // don't double-count this as a resolve
    dest->passthrough_requests++;

    if (h->nhosts == 0) {
        pthread_mutex_unlock(&destlk);
        goto done;
    }

    buf = strbuf_new();
    strbuf_append(buf, "@");
    for (int i = 0; i < h->nhosts; i++) {
        strbuf_appendf(buf, "%s:dc=%s", h->hosts[i], h->rooms[i]);
        if (i != h->nhosts-1)
            strbuf_append(buf, ",");
    }
    pthread_mutex_unlock(&destlk);
    int rc;

    cdb2_hndl_tp *db = NULL;
    rc = cdb2_open(&db, dbname, strbuf_buf(buf), CDB2_FORCE_CONNECT | CDB2_KEEP_FD);
    if (rc) {
        if (db)
            cdb2_close(db);
        goto done;
    }
    fd = cdb2_fileno(db);
    cdb2_close(db);
    printf("returning from passthrough\n");

done:
    if (buf)
        strbuf_free(buf);
    free(s);
    return fd;
}

void discover_fallback_check(void) {
    time_t now = time(NULL);
    printf("fallback check: errors %d (last %d seconds ago) ok %d  emergency? %c\n", 
            (int) comdb2db_errors, 
            last_error ? now - last_error : 0,
            (int) comdb2db_ok,
            emergency_passthrough ? 'Y' : 'N'
            );

    pthread_mutex_lock(&destlk);
    if (!emergency_passthrough && comdb2db_errors > error_threshold) {
        fprintf(stderr, "Too many comdb2db errors, enabling emergency passthrough\n");
        emergency_passthrough = 1;
        comdb2db_errors = 0;
        comdb2db_ok = 0;
    }
    if (emergency_passthrough) {
        if (comdb2db_ok > recover_threshold) {
            fprintf(stderr, "comdb2db back to normal, disabling emergency passthrough\n");
            emergency_passthrough = 0;
            comdb2db_ok = 0;
        }
    }
    if (now - last_error > 60) {
        comdb2db_errors = 0;
    }
    pthread_mutex_unlock(&destlk);
}
