#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"

typedef struct delfile_logref {
    char *name;
    int last_ref;
} delfile_logref_t;

static int get_references(void **data, int *npoints)
{
    int rc, bdberr = 0, nkeys;
    char **files;
    int *logs;
    delfile_logref_t *refs = NULL;
    tran_type *tran = curtran_gettran();

    rc = bdb_llmeta_get_delfile_logrefs(tran, &files, &logs, &nkeys, &bdberr);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to get delete-file log references\n",
               __func__);
        return SQLITE_INTERNAL;
    }

    refs = calloc(nkeys, sizeof(struct delfile_logref));
    if (refs == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc\n", __func__);
        return SQLITE_NOMEM;
    }

    for (int i = 0; i < nkeys; i++) {
        refs[i].name = files[i];
        refs[i].last_ref = logs[i];
    }

    free(files);
    free(logs);

    *npoints = nkeys;
    *data = refs;
    return 0;
}

static void free_references(void *p, int n)
{
    delfile_logref_t *refs = p;
    for (int i = 0; i < n; i++) {
        free(refs[i].name);
    }
    free(refs);
}

sqlite3_module systblDelfileRefsModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblDelfileRefsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_delfile_logrefs", &systblDelfileRefsModule,
        get_references, free_references, sizeof(struct delfile_logref),
        CDB2_CSTRING, "file", -1, offsetof(struct delfile_logref, name),
        CDB2_INTEGER, "lastref", -1, offsetof(struct delfile_logref, last_ref),
        SYSTABLE_END_OF_FIELDS);
}
