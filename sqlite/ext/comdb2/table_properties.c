/*
   Copyright 2021 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include "comdb2.h"
#include "sqlite3.h"
#include "ezsystables.h"

extern struct dbenv *thedb;

sqlite3_module systblTablePropertiesModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables",
};

typedef struct systable_table_properties {
    char *table_name;
    const char *odh;
    const char *mvcc;
    const char *compress;
    const char *blob_compress;
    const char *in_place_updates;
    const char *instant_schema_change;
} systable_table_properties_t;

int table_properties_systable_collect(void **data, int *nrecords)
{
    int odh, mvcc, compr, blob_compr;

    *nrecords = thedb->num_dbs;
    systable_table_properties_t *arr = calloc(*nrecords, sizeof(systable_table_properties_t));
    for (int i = 0; i < *nrecords; i++) {
        struct dbtable *db = thedb->dbs[i];
        bdb_get_compr_flags(db->handle, &odh, &mvcc, &compr, &blob_compr);

        arr[i].table_name = strdup(db->tablename);
        arr[i].odh = odh ? "Y" : "N";
        arr[i].mvcc = mvcc ? "Y" : "N";
        arr[i].compress = bdb_algo2compr(compr);
        arr[i].blob_compress = bdb_algo2compr(blob_compr);
        arr[i].in_place_updates = db->inplace_updates ? "Y" : "N";
        arr[i].instant_schema_change = db->instant_schema_change ? "Y" : "N";
    }

    *data = arr;
    return 0;
}

void table_properties_systable_free(void *arr, int nrecords)
{
    systable_table_properties_t *parr = (systable_table_properties_t *)arr;
    int i;

    for (i = 0; i < nrecords; i++) {
        free(parr[i].table_name);
    }
    free(arr);
}

int systblTablePropertiesInit(sqlite3*db)
{
    return create_system_table(
        db, "comdb2_table_properties", &systblTablePropertiesModule,
        table_properties_systable_collect, table_properties_systable_free,
        sizeof(systable_table_properties_t),
        CDB2_CSTRING, "table_name", -1, offsetof(systable_table_properties_t, table_name),
        CDB2_CSTRING, "odh", -1, offsetof(systable_table_properties_t, odh),
        CDB2_CSTRING, "mvcc", -1, offsetof(systable_table_properties_t, mvcc),
        CDB2_CSTRING, "compress", -1, offsetof(systable_table_properties_t, compress),
        CDB2_CSTRING, "blob_compress", -1, offsetof(systable_table_properties_t, blob_compress),
        CDB2_CSTRING, "in_place_updates", -1, offsetof(systable_table_properties_t, in_place_updates),
        CDB2_CSTRING, "instant_schema_change", -1, offsetof(systable_table_properties_t, instant_schema_change),
        SYSTABLE_END_OF_FIELDS);
}
