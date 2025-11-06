/*
   Copyright 2025 Bloomberg Finance L.P.

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

#include "comdb2_plugin.h"
#include "comdb2.h"
#include "bdb_int.h"
#include "comdb2_appsock.h"
#include <build/db.h>
#include <build/db_int.h>
#include <dbinc/log.h>
#include "unistd.h"

comdb2_appsock_t logfill_plugin;

static int retrieve_range(struct sbuf2 *sb, uint32_t gen, int sfile, int soffset, int efile, int eoffset)
{
    bdb_state_type *bdb_state = thedb->bdb_env;
    DB_LOGC *logc = NULL;
    DB_LSN lsn = {.file = sfile, .offset = soffset}, elsn = {.file = efile, .offset = eoffset};
    DBT data = {0};
    uint32_t my_gen;
    int64_t cnt = 0;
    int rc;

    if (BDB_TRYREADLOCK("logfill_retrieve_range") != 0) {
        logmsg(LOGMSG_ERROR, "logfill retrieve_range failed to get readlock\n");
        sbuf2printf(sb, "ERROR could not get bdb readlock\n");
        sbuf2flush(sb);
        return -1;
    }

    if ((rc = bdb_state->dbenv->log_cursor(bdb_state->dbenv, &logc, 0)) != 0) {
        logmsg(LOGMSG_ERROR, "logfill retrieve_range failed to get log cursor rc=%d\n", rc);
        sbuf2printf(sb, "ERROR could not get log cursor\n");
        sbuf2flush(sb);
        return -1;
    }

    logc->setflags(logc, DB_LOG_SILENT_ERR);

    bdb_state->dbenv->get_rep_gen(bdb_state->dbenv, &my_gen);
    if (my_gen != gen) {
        BDB_RELLOCK();
        logc->close(logc, 0);
        logmsg(LOGMSG_ERROR, "logfill retrieve_range generation mismatch %u != %u\n", my_gen, gen);
        sbuf2printf(sb, "ERROR generation mismatch %u != %u\n", my_gen, gen);
        sbuf2flush(sb);
        return -1;
    }

    data.flags = DB_DBT_REALLOC;

    while (!bdb_lock_desired(bdb_state) && log_compare(&lsn, &elsn) <= 0 && !rc) {
        uint32_t flags = (cnt == 0) ? DB_SET : DB_NEXT;
        if ((rc = logc->get(logc, &lsn, &data, flags)) == 0) {
            sbuf2printf(sb, "LOG %u %u %u\n", lsn.file, lsn.offset, (unsigned int)data.size);
            sbuf2write(data.data, data.size, sb);
            sbuf2printf(sb, "\n");
        }
        cnt++;
    }

    BDB_RELLOCK();
    logc->close(logc, 0);
    sbuf2printf(sb, "END\n", cnt);
    sbuf2flush(sb);
    if (data.data)
        free(data.data);
    return 0;
}

static int handle_logfill_request(comdb2_appsock_arg_t *arg)
{
    struct thr_handle *thr_self;
    struct sbuf2 *sb;
    char line[128] = {0};

    thr_self = arg->thr_self;
    sb = arg->sb;

    thrman_change_type(thr_self, THRTYPE_LOGFILL);
    sbuf2settimeout(sb, 0, 0);

    while (sbuf2gets(line, sizeof(line), sb) > 0) {
        static const char *delims = " \r\t\n";
        char *lasts;
        char *tok;

        tok = strtok_r(line, delims, &lasts);
        if (!tok) {
            continue;

        // getrange <generation> <startfile> <startoffset> <endfile> <endoffset>
        } else if (strcmp(tok, "getrange") == 0) {
            uint32_t generation;
            int sfile, soffset, efile, eoffset;

            // generation
            tok = strtok_r(NULL, delims, &lasts);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "logfill thread missing generation\n");
                sbuf2printf(sb, "ERROR missing generation\n");
                sbuf2flush(sb);
                continue;
            }
            generation = strtoul(tok, NULL, 10);

            // startfile
            tok = strtok_r(NULL, delims, &lasts);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "logfill thread missing start-file\n");
                sbuf2printf(sb, "ERROR missing start-file\n");
                sbuf2flush(sb);
                continue;
            }
            sfile = strtol(tok, NULL, 10);

            // startoffset
            tok = strtok_r(NULL, delims, &lasts);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "logfill thread missing start-offset\n");
                sbuf2printf(sb, "ERROR missing start-offset\n");
                sbuf2flush(sb);
                continue;
            }
            soffset = strtol(tok, NULL, 10);

            // endfile
            tok = strtok_r(NULL, delims, &lasts);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "logfill thread missing end-file\n");
                sbuf2printf(sb, "ERROR missing end-file\n");
                sbuf2flush(sb);
                continue;
            }
            efile = strtol(tok, NULL, 10);

            // endoffset
            tok = strtok_r(NULL, delims, &lasts);
            if (!tok) {
                logmsg(LOGMSG_ERROR, "logfill thread missing end-offset\n");
                sbuf2printf(sb, "ERROR missing end-offset\n");
                sbuf2flush(sb);
                continue;
            }
            eoffset = strtol(tok, NULL, 10);
            retrieve_range(sb, generation, sfile, soffset, efile, eoffset);
        } else {
            logmsg(LOGMSG_ERROR, "logfill thread got unknown token <%s>\n",
                   tok);
        }
    }
    return APPSOCK_RETURN_OK;
}

comdb2_appsock_t logfill_plugin = {
    "logfill",               /* Name */
    "",                      /* Usage info */
    0,                       /* Execution count */
    0,                       /* Flags */
    handle_logfill_request /* Handler function */
};

#include "plugin.h"
