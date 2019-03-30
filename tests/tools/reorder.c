#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <cdb2api.h>

char *argv0 = NULL;
char *dbname = NULL;
char *dest = "local";
int txnsize = 2;
int iterations = 1000;
int maxrange = 20000;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s <options>\n", argv0);
    fprintf(f, "    -d <dbname>\n");
    fprintf(f, "    -r <maxrange>\n");
    fprintf(f, "    -t <threadcound>\n");
    fprintf(f, "    -x <txnsize>\n");
    fprintf(f, "    -i <iterations>\n");
    exit(1);
}

void *insert_random(void *x)
{
    int rc;
    cdb2_hndl_tp *hndl = NULL;
    if ((rc = cdb2_open(&hndl, dbname, dest, 0))) {
        fprintf(stderr, "Error opening handle for %s, rc = %d\n", dbname, rc);
        exit(1);
    }
    for (int i = 0; i < iterations; i++) {
        if ((rc = cdb2_run_statement(hndl, "begin"))) {
            fprintf(stderr, "Error running begin, %s, rc = %d\n", cdb2_errstr(hndl), rc);
            exit(1);
        }
        for (int j = 0; j < txnsize; j++) {
            char sql[64];
            snprintf(sql, sizeof(sql), "insert into t1 (a, b) values(%d, %d)",
                    rand() % maxrange, rand() % maxrange);
            if ((rc = cdb2_run_statement(hndl, sql))) {
                fprintf(stderr, "Error running insert, %s, rc = %d\n", cdb2_errstr(hndl), rc);
                exit(1);
            }
        }
        if ((rc = cdb2_run_statement(hndl, "commit"))) {
            fprintf(stderr, "Error running commit, %s, rc = %d\n", cdb2_errstr(hndl), rc);
            exit(1);
        }
    }
    cdb2_close(hndl);
    return NULL;
}

int main(int argc, char *argv[])
{
    int rc, c, errors = 0, threads = 5;
    pthread_t *thds = NULL;
    argv0 = argv[0];
    srand(time(NULL) * getpid());
    while ((c = getopt(argc, argv, "d:t:x:i:")) != EOF) {
        switch(c) {
            case 'd':
                dbname = optarg;
                break;
            case 't':
                threads = atoi(optarg);
                break;
            case 'x':
                txnsize = atoi(optarg);
                break;
            case 'i':
                iterations = atoi(optarg);
                break;
            case 'r':
                maxrange = atoi(optarg);
                break;
            default:
                fprintf(stderr, "Unknown option '%c'\n", optopt);
                errors++;
        }
    }

    if (errors || dbname == NULL)
        usage(stderr);

    char *conf = getenv("CDB2_CONFIG");
    if (conf) {
        cdb2_set_comdb2db_config(conf);
        dest = "default";
    }

    thds = (pthread_t *)calloc(sizeof(pthread_t), threads);
    for (int i = 0; i < threads; i++) {
        if ((rc = pthread_create(&thds[i], NULL, insert_random, NULL)) != 0) {
            fprintf(stderr, "pthread_create error: %d\n", rc);
            exit(1);
        }
    }
    for (int i = 0; i < threads; i++) 
        pthread_join(thds[i], NULL);
    return 0;
}
