#include <stdio.h>
#include <strings.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <cdb2api.h>

char *argv0 = NULL;
char *dbname = NULL;
char *dest = "local";
int txnsize = 2;
int iterations = 1000;
int columns = 2;
int maxrange = 20000;

void usage(FILE *f)
{
    fprintf(f, "Usage: %s <options>\n", argv0);
    fprintf(f, "    -d <dbname>\n");
    fprintf(f, "    -r <maxrange>\n");
    fprintf(f, "    -t <threadcound>\n");
    fprintf(f, "    -x <txnsize>\n");
    fprintf(f, "    -c <num-columns>\n");
    fprintf(f, "    -i <iterations>\n");
    exit(1);
}

void *insert_random(void *x)
{
    int rc;
    char *sql = (char *)malloc(64 + (columns * 100));
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

            sql[0] = '\0';
            strcat(sql, "insert into t1(");
            for (int c = 0; c < columns ;c++) {
                char col[6];
                if (c == 0) {
                    sprintf(col, "a%d", c);
                    strcat(sql, col);
                }
                else {
                    sprintf(col, ",a%d", c);
                    strcat(sql, col);
                }
            }
            strcat(sql, ") values (");
            for (int c = 0; c < columns ;c++) {
                char val[12];
                if (c == 0) {
                    sprintf(val, "%d", rand() % maxrange);
                    strcat(sql, val);
                } else {
                    sprintf(val, ",%d", rand() % maxrange);
                    strcat(sql, val);
                }
            }
            strcat(sql, ")");

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

int create_table(int columns)
{
    int rc;
    /* "create table t1 (a1, a2, a3, a4, a5..)" */
    char *sql = (char *)malloc(64 + (columns * 50));
    cdb2_hndl_tp *hndl = NULL;
    if ((rc = cdb2_open(&hndl, dbname, dest, 0))) {
        fprintf(stderr, "Error opening handle for %s, rc = %d\n", dbname, rc);
        exit(1);
    }

    sql[0] = '\0';
    strcat(sql, "create table t1(");
    for (int i = 0; i < columns; i++) {
        char colname[20];
        if (i == 0) {
            sprintf(colname, "a%d int", i);
            strcat(sql, colname);
        } else {
            sprintf(colname, ",a%d int", i);
            strcat(sql, colname);
        }
    }
    strcat(sql, ")");

    if ((rc = cdb2_run_statement(hndl, sql))) {
        fprintf(stderr, "Error running create, %s, %s rc = %d\n", cdb2_errstr(hndl), sql, rc);
        exit(1);
    }

    sql[0] = '\0';

    for (int i = 0; i < columns; i++) {
        sprintf(sql, "create index ix%d on t1(a%d)", i, i);
        if ((rc = cdb2_run_statement(hndl, sql))) {
            fprintf(stderr, "Error creating index, %s, rc = %d\n", cdb2_errstr(hndl), rc);
            exit(1);
        }
    }

    cdb2_close(hndl);
    free(sql);
    return 0;
}

int main(int argc, char *argv[])
{
    int rc, c, errors = 0, threads = 5;
    pthread_t *thds = NULL;
    argv0 = argv[0];
    srand(time(NULL) * getpid());
    while ((c = getopt(argc, argv, "d:t:x:i:c:")) != EOF) {
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
            case 'c':
                columns = atoi(optarg);
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

    create_table(columns);

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
