#include <cstring>

#include "comdb2ar.h"
#include "error.h"
#include "file_info.h"
#include "fdostream.h"
#include "lrlerror.h"
#include "tar_header.h"
#include "riia.h"
#include "increment.h"
#include "util.h"

#include <cstdlib>
#include <map>
#include <set>
#include <string>
#include <sstream>
#include <iostream>
#include <fstream>
#include <thread>
#include <mutex>
#include <vector>
#include <memory>

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/statvfs.h>
#include <dirent.h>

#include <sys/wait.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>

/* check once a megabyte */
#define FS_PERIODIC_CHECK (10 * 1024 * 1024)

static bool check_dest_dir(const std::string& dir);

static void remove_old_files(const std::list<std::string>& dirlist,
        const std::set<std::string>& extracted_files,
        const std::string& pattern)
// Remove files in the directory listing dirlist that match the given file
// pattern, provided that we have not already overwritten them with new files
// (i.e. do not remove any file that appears in the extracted_files set).
// All inputs to this function should be absolute paths.
// pattern should be an absolute path optionally ending in '*' to indicate
// a wildcard.
{
    size_t matchlen = pattern.length();
    bool exact_match = true;
    if(matchlen > 0 && pattern[matchlen-1] == '*') {
        exact_match = false;
        matchlen--;
    }
    for(std::list<std::string>::const_iterator ii = dirlist.begin();
            ii != dirlist.end();
            ++ii) {

        if((exact_match && ii->compare(pattern)==0)
                ||
           (!exact_match && ii->compare(0, matchlen, pattern, 0, matchlen)==0)
           ) {

            if(extracted_files.find(*ii) == extracted_files.end()) {
                if(-1 == unlink(ii->c_str())) {
                    std::cerr << "Error unlinking " << *ii << ": "
                        << strerror(errno) << std::endl;
                } else {
                    std::clog << "unlinked " << *ii << std::endl;
                }
            }
        }
    }
}



bool read_octal_ull(const char *str, size_t len, unsigned long long& number)
{
    if(*str == '\200') {
        /* This is encoded in base 256 as per the GNU extensions */
        number = 0;
        len--;
        str++;
        while(len) {
            number = (number << 8) | *((unsigned char *)str);
            len--;
            str++;
        }
    } else {
        char *endptr = NULL;

        if(str[len - 1] != '\0') {
            return false;
        }

        errno = 0;
        number = strtoull(str, &endptr, 8);
        if(errno != 0 || !endptr || *endptr != '\0') {
            return false;
        }
    }
    return true;
}

static void check_remaining_space(const std::string &datadestdir,
        unsigned percent_full, const std::string &filename,
        unsigned long long bytesleft)
{
    struct statvfs stfs;
    int rc = statvfs(datadestdir.c_str(), &stfs);
    if(rc == -1) {
        std::ostringstream ss;
        ss << "Error running statvfs on " << datadestdir
            << ": " << strerror(errno);
        throw Error(ss);
    }

    fsblkcnt_t fsblocks = bytesleft / stfs.f_bsize;
    double percent_free = 100.00 * ((double)(stfs.f_bavail - fsblocks) / (double)stfs.f_blocks);
    if(100.00 - percent_free >= percent_full) {
        std::ostringstream ss;
        ss << "Not enough space to deserialise " << filename
            << " (" << bytesleft << " bytes) - would leave only "
            << percent_free << "% free space";
        throw Error(ss);
    }
}


static void process_manifest(
        const std::string& text,
        std::map<std::string, FileInfo>& manifest_map,
        bool& run_full_recovery,
        std::string& origlrlname,
        std::vector<std::string> &options
        )
// Decode the manifest into a map of files and their associated file info
{
    FileInfo tmp_file;

    std::istringstream in(text);
    std::string line;

    int lineno = 0;
    while(!in.eof()) {
        std::getline(in, line);
        lineno++;

        size_t pos = line.find_first_of('#');
        if(pos != std::string::npos) {
            line.resize(pos);
        }

        std::istringstream ss(line);
        std::string tok;

        if(ss >> tok) {
            if(tok == "File") {
                if(read_FileInfo(line, tmp_file)) {
                    manifest_map[tmp_file.get_filename()] = tmp_file;
                    tmp_file.reset();
                } else {
                    std::clog << "Bad File directive on line "
                        << lineno << " of MANIFEST" << std::endl;
                }
            } else if(tok == "SupportFilesOnly") {
                // If serialisation contains sources only then no need to run
                // full recovery.
                run_full_recovery = false;
            } else if(tok == "OrigLrlFile") {

               // llmeta renames the lrl file
               // metafile contains the original file name
               // correct this here
               ss >> origlrlname;
            } else if (tok == "Option") {
                while (ss >> tok) {
                    options.push_back(tok);
                }
            } else {
                std::clog << "Unknown directive '" << tok << "' on line "
                    << lineno << " of MANIFEST" << std::endl;
            }
        }
    }
}


static void process_lrl(
        std::ostream& of,
        const std::string& filename,
        const std::string& lrltext,
        bool strip_cluster_info,
        bool strip_consumer_info,
        std::string& datadestdir,
        const std::string& lrldestdir,
        std::string& dbname,
        std::set<std::string>& table_set
        )
// Parse the lrl file that has been read from the stream, set database
// parameters as they are discovered, and write out a rewritten lrl file
// to the output stream of.  dir, table and resource lines will be rewritten
// with the new data directory.
// dbname and datadestdir, if empty, will be populated with the values read
// from the lrltext.
// table_set will be updated with the names of any tables found
{
    std::istringstream lrlss(lrltext);
    int lineno = 0;
    while(!lrlss.eof()) {
        std::string line;
        std::getline(lrlss, line);
        lineno++;

        std::string bareline(line);
        size_t pos = bareline.find_first_of('#');
        if(pos != std::string::npos) {
            bareline.resize(pos);
        }
        std::istringstream liness(bareline);

        std::string tok;
        if(liness >> tok) {

            if(tok == "name" && dbname.empty()) {
                // Get the db name from the first lrl file found,
                // which should be the primary.
                if(!(liness >> dbname)) {
                    throw LRLError(filename, lineno, "missing database name directive");
                }
            } else if(tok == "dir") {
                if(!(liness >> tok)) {
                    throw LRLError(filename, lineno, "missing database directory");
                }
                // If we don't have a directory yet then use this one
                if(datadestdir.empty()) {
                    datadestdir = tok;
                }
                line = "dir " + datadestdir;

            } else if(tok == "table") {
                std::string name;
                std::string schema_path, new_path;
                std::string dbnum;
                if(!(liness >> name)) {
                    throw LRLError(filename, lineno, "missing table name");
                }
                if(!(liness >> schema_path)) {
                    throw LRLError(filename, lineno, "missing table schema path");
                }
                liness >> dbnum; // optional database number

                std::string ext;
                size_t dot_pos = schema_path.find_last_of('.');
                if(dot_pos != std::string::npos) {
                    ext = schema_path.substr(dot_pos + 1);
                }
                makebasename(schema_path);
                if(ext == "lrl") {
                    // lrl files get put in the lrl destination dir..
                    makeabs(new_path, lrldestdir, schema_path);
                } else {
                    if(datadestdir.empty()) {
                        throw LRLError(filename, lineno, "table directive before dir; cannot infer data directory");
                    }
                    makeabs(new_path, datadestdir, schema_path);
                }

                line = "table " + name + " " + new_path + " " + dbnum;

                table_set.insert(name);

            } else if(tok == "resource") {
                std::string path, new_path, name;
                if(!(liness >> name >> path)) {
                    throw LRLError(filename, lineno, "missing resource name or path");
                }

                if(datadestdir.empty()) {
                    throw LRLError(filename, lineno, "resource directive before dir; cannot infer data directory");
                }

                makebasename(path);
                makeabs(new_path, datadestdir, path);

                line = "resource " + name + " " + new_path;
            } else if(tok == "spfile") {
                std::string path,new_path;
                if(!(liness >> path)) {
                    throw LRLError(filename, lineno, "missing spfile");
                }
                makebasename(path);
                makeabs(new_path, datadestdir, path);
                line = "spfile " + new_path;
            } else if(tok == "timepartitions") {
                std::string path,new_path;
                if(!(liness >> path)) {
                    throw LRLError(filename, lineno, "missing timepartitions file");
                }
                makebasename(path);
                makeabs(new_path, datadestdir, path);
                line = "timepartitions " + new_path;
            } else if(tok == "cluster" && (liness >> tok)
                    && tok == "nodes" && strip_cluster_info) {
                // Strip this line from the output.  Since this test above
                // changes tok, it needs to be the last test
                line.insert(0, "# ");
            } else if (strip_consumer_info) {
                if (tok == "if") {
                    liness >> tok; /* consume machine type */
                    liness >> tok; /* read real option */
                }

                if (tok == "queue" || tok == "procedure" || tok == "consumer")
                    line.insert(0, "# ");

                /* Also strip ssl config from the LRL for QA mode. */
                if(strncasecmp(tok.c_str(), "ssl", 3) == 0)
                    line.insert(0, "# ");
            }
        }

        if(!(of << line << std::endl)) {
            std::ostringstream ss;
            ss << "Error writing " << filename;
            throw Error(ss);
        }
    }
}

static bool empty_dir(const std::string& dir) {
    DIR *d;
    struct dirent *f;
    d = opendir(dir.c_str());
    if (d == NULL)
        return false;
    while ((f = readdir(d)) != NULL) {
        if (strcmp(f->d_name, ".") == 0 || strcmp(f->d_name, "..") == 0)
            continue;
        return false;
    }
    return true;
}

static bool check_dest_dir(const std::string& dir)
// REALLY REALLY REALLY important safety check - without this
// we may end up deleting *.dta from /bb/data!
// Only allow 3rd level subdirectories or greater (e.g. /bb/data/mydb)
{
    size_t pos = 0;
    int levels = 0;

    /* skip the check if the destination directory is empty */
    if (empty_dir(dir))
        return false;

    while(pos < dir.length()) {
        size_t match = dir.find('/', pos);

        if(match == std::string::npos) {
            // No more matches
            match = dir.length();
        }

        if(match > pos) {
            levels++;
        }

        pos = match + 1;
    }

    if(levels < 3) return false;

    return true;
}

#define write_size (1000*1024)

void do_write_thd(const std::string filename, std::mutex *lk,
                const std::string destdir, unsigned long long *current_offset,
                unsigned long long filesize, size_t bufsize,
                bool file_is_sparse, unsigned long long *padding_bytes,
                unsigned percent_full, bool direct)
{
    uint8_t *buf;
    unsigned long long recheck_count = FS_PERIODIC_CHECK;
    unsigned long long readbytes = 0;
    unsigned long long skipped_bytes = 0;
    unsigned long long padding;
    off_t my_offset;
    //unsigned long long last
    std::string outfilename(destdir + "/" + filename);

    void *empty_page = malloc(65536);
    memset(empty_page, 0, 65536);
    
    std::unique_ptr<fdostream> of_ptr;

    of_ptr = output_file(outfilename, false, direct, false);
#if defined _HP_SOURCE || defined _SUN_SOURCE
    buf = (uint8_t*) memalign(512, bufsize);
#else
    if (posix_memalign((void**) &buf, 512, bufsize))
        throw Error("Failed to allocate output buffer");
#endif
    RIIA_malloc free_guard(buf);

    while (true) {
        (*lk).lock();
        my_offset = (*current_offset);
        unsigned long long bytesleft = filesize - my_offset;

        /* Set amount to read */
        readbytes = (bytesleft > bufsize) ? bufsize : bytesleft;

        /* Read */
        if(readall(0, &buf[0], readbytes) != readbytes) {
            std::ostringstream ss;

            ss << "Error reading " << readbytes << " bytes for file "
                << filename << " after "
                << (filesize - bytesleft) << " bytes, with "
                << bytesleft << " bytes left to read:"
                << errno << " " << strerror(errno);
            throw Error(ss);
        }

        (*current_offset) += readbytes;
        (*lk).unlock();
        
        bytesleft -= readbytes;
        if (bytesleft == 0) {
            break;
        }

        /* Skipping sparse bytes */
        if (file_is_sparse && (readbytes == bufsize) && memcmp(empty_page,
                    &buf[0], bufsize) == 0) {
            skipped_bytes += bufsize;
            continue;
        }

        recheck_count -= readbytes;

        uint64_t bytes = readbytes;
        uint64_t off = 0;
        //int fd = of_ptr
        of_ptr->setoffset(my_offset);

        /* Write out in a loop */
        while (bytes > 0) {
            int lim = (bytes < write_size) ? bytes : write_size;

            if (!of_ptr->write((char*) &buf[off], lim))
            {
                std::ostringstream ss;
                ss << "Error Writing " << filename << " after "
                    << (filesize - bytesleft) << " bytes";
                throw Error(ss);
            }
            off += lim;
            bytes -= lim;
        }

        /* Check remaining space */
        if (recheck_count <= 0) {
            check_remaining_space(destdir, percent_full, filename, bytesleft);
            recheck_count = FS_PERIODIC_CHECK;
        }
    }


    (*lk).lock();
    padding = *padding_bytes;
    (*padding_bytes) = 0;
    (*lk).unlock();


    if (padding) {
        if(readall(0, &buf[0], padding) != padding) {
            std::ostringstream ss;
    
            ss << "Error reading padding after " << filename
                << ": " << errno << " " << strerror(errno);
            throw Error(ss);
        }
    }

    free(empty_page);
}

void thd_test(unsigned long long *num) {
    return;
}

void do_write(const std::string filename, const std::string destdir,
                unsigned long long bytesleft, size_t bufsize,
                bool file_is_sparse, unsigned long long padding_bytes,
                unsigned percent_full, bool direct, int deserialization_threads)
{
    unsigned long long current_offset = 0;
    std::mutex *lk = new std::mutex();;
    std::thread *thds = { new std::thread[deserialization_threads]{} };
    /*
    for (int i = 0; i < deserialization_threads; i++) {
        thds[i] = std::thread(thd_test, current_offset);
    }
    */
    for (int i = 0; i < deserialization_threads; i++) {
        thds[i] = std::thread(do_write_thd, filename, lk, destdir,
                &current_offset, bytesleft, bufsize, file_is_sparse,
                &padding_bytes, percent_full, direct);
    }

    for (int i = 0; i < deserialization_threads; i++) {
        thds[i].join();
    }
}

void deserialise_database(
        const std::string *p_lrldestdir,
        const std::string *p_datadestdir,
        bool strip_cluster_info,
        bool strip_consumer_info,
        bool run_full_recovery,
        const std::string& comdb2_task,
        unsigned percent_full,
        bool force_mode,
        bool legacy_mode,
        bool& is_disk_full,
        int deserialization_threads,
        bool run_with_done_file,
        bool incr_mode,
        bool dryrun
)
// Deserialise a database from serialised from received on stdin.
// If lrldestdir and datadestdir are not NULL then the lrl and data files
// will be placed accordingly.  Otherwise the lrl file and the data files will
// be placed in the same directory as specified in the serialised form.
// The lrl file written out will be updated to reflect the resulting directory
// structure.  If the destination disk reaches or exceeds the specified
// percent_full during the deserialisation then the operation is halted.
{
    static const char zero_head[512] = {0};
    int stlen;
    is_disk_full = false;
    void *empty_page = NULL;
    unsigned long long skipped_bytes = 0;
    int rc =0;
    std::vector<std::string> options;

    bool file_is_sparse = false;

    std::string fingerprint;

    std::string done_file_string;

    // This is the directory listing of the data directory taken at the time
    // when we find the lrl file in out input stream and can therefore fix
    // the data directory.
    std::list<std::string> data_dir_files;

    // The absolute path names of the files that we have already extracted.
    std::set<std::string> extracted_files;

    // List of known tables
    std::set<std::string> table_set;

    std::string datadestdir;

    std::string fullpath;

    empty_page = malloc(65536);
    memset(empty_page, 0, 65536);

    if (p_datadestdir == NULL && p_lrldestdir)
        p_datadestdir = p_lrldestdir;

    if(p_datadestdir) {
        datadestdir = *p_datadestdir;
        stlen=datadestdir.length();
        if( datadestdir[stlen - 1] == '/' )
        {
            datadestdir.resize( stlen - 1 );
        }
    }
    const std::string& lrldestdir = p_lrldestdir ? *p_lrldestdir : datadestdir;

    bool inited_txn_dir = false;
    std::string copylock_file;
    std::string main_lrl_file;
    std::string dbname;
    std::string origlrlname("");

    std::string sha_fingerprint = "";

    // The manifest map
    std::map<std::string, FileInfo> manifest_map;

    if (run_with_done_file)
    {
       /* remove the DONE file before we start copying */
       done_file_string = datadestdir + "/DONE";
       unlink(done_file_string.c_str());
    }


    while(true) {

        if(!datadestdir.empty() && !dbname.empty() && !inited_txn_dir) {
            // Init the .txn dir.  We need to make sure that we remove:
            // * any queue extent files lying around
            // * any log files lying around
            // * __db.rep.db
            // Any database like files from the destination directory dir

            if(datadestdir[0] != '/') {
                std::ostringstream ss;
                ss << "Cannot deserialise into " << datadestdir
                    << " - destination directory must be an absolute path";
                throw Error(ss);
            }

            // Create the copylock file to indicate that this copy isn't
            // done yet.
            copylock_file = lrldestdir + "/" + dbname + ".copylock";
#if 0
            struct stat statbuf;
            rc = stat(copylock_file.c_str(), &statbuf);
            if (rc == 0)
            {
               std::ostringstream ss;
               ss << "Error copylock file exists " << copylock_file
                  << " " << std::strerror(errno);
               throw Error(ss);
            }
#endif
            unlink(copylock_file.c_str());
            int fd = creat(copylock_file.c_str(), 0666);
            if(fd == -1) {
                std::ostringstream ss;
                ss << "Error creating copylock file " << copylock_file
                    << " " << strerror(errno);
                throw Error(ss);
            }

            // remove logs
            std::string logsdir = datadestdir + "/logs";
            std::list<std::string> logfiles;
            listdir(logfiles, logsdir);
            for (std::list<std::string>::const_iterator it = logfiles.begin();
                    it != logfiles.end();
                    ++it) {
                std::string log = logsdir + "/" + *it;
                std::cout << "unlink " << log << std::endl;
                unlink(log.c_str());
            }

            inited_txn_dir = true;
        }

        // Read the tar block header
        tar_block_header head;
        if(readall(0, head.c, sizeof(head.c)) != sizeof(head.c)) {
            // Failed to read a full block header
            std::ostringstream ss;
            ss << "Error reading tar block header: "
                << errno << " " << strerror(errno);
            throw Error(ss);
        }

        // If the block is entirely blank then we're done
        // Alternativelyh, if we're running in incremental mode, then
        // we know we are moving on the the incremental backups
        if(std::memcmp(head.c, zero_head, 512) == 0) {
            if(incr_mode){
                std::clog << "Done with base backup, moving on to increments"
                          << std::endl << std::endl;

                incr_deserialise_database(
                    lrldestdir,
                    datadestdir,
                    dbname,
                    table_set,
                    sha_fingerprint,
                    percent_full,
                    force_mode,
                    options,
                    is_disk_full,
                    dryrun
                );
            }
            break;
        }

        // TODO: verify the block check sum

        // Get the file name
        if(head.h.filename[sizeof(head.h.filename) - 1] != '\0') {
            throw Error("Bad block: filename is not null terminated");
        }
        const std::string filename(head.h.filename);

        if (filename == "FLUFF")
            return;

        // Try to find this file in our manifest
        std::map<std::string, FileInfo>::const_iterator manifest_it = manifest_map.find(filename);

        // Get the file size
        unsigned long long filesize;
        if(!read_octal_ull(head.h.size, sizeof(head.h.size), filesize)) {
            throw Error("Bad block: bad size");
        }
        unsigned long long nblocks = (filesize + 511ULL) >> 9;

        // Calculate padding bytes
        unsigned long long padding_bytes = (nblocks << 9) - filesize;

        // If this is an .lrl file then we have to read it into memory and
        // then rewrite it to disk.  In getting the extension it is important
        // to use the **first** dot, as this will affect the data file
        // recognition code below where we don't want "fstblk.dta" to
        // match the pattern "dta*"
        std::string ext;
        size_t dot_pos = filename.find_first_of('.');
        bool is_lrl = false;
        if(dot_pos != std::string::npos) {
            ext = filename.substr(dot_pos + 1);
        }
        if(ext == "lrl") {
            is_lrl = true;
        }

        if(ext == "sha") {
            sha_fingerprint = read_serialised_sha_file();
            continue;
        }

        bool is_manifest = false;
        if(filename == "MANIFEST") {
            is_manifest = true;
        }

        // Gather the text of the file for lrls and manifests
        std::string text;
        bool is_text = is_lrl || is_manifest;

        // If it's not a text file then look at the extension to see if it looks
        // like a data file.  If it does then add it to our list of tables if
        // not already present.  I was going to trust the lrl file for this
        // (and use the dbname_file_vers_map for llmeta dbs) but the old
        // comdb2backup script doesn't serialise the file_vers_map, so I can't
        // do that yet. This way the onus is on the serialising side to get
        // the list of files right.
        if(!is_text && filename.find_first_of('/') == std::string::npos) {
            bool is_data_file = false;
            bool is_queue_file = false;
            bool is_queuedb_file = false;
            std::string table_name;

            if(recognize_data_file(filename, is_data_file,
                        is_queue_file, is_queuedb_file, table_name)) {
                if(table_set.insert(table_name).second) {
                    std::clog << "Discovered table " << table_name
                        << " from data file " << filename << std::endl;
                }
            }
        }

        std::unique_ptr<fdostream> of_ptr;
        bool direct;

        if(is_text) {
            text.reserve(filesize);
        } else {
            // Verify that we will have enough disk space for this file
            struct statvfs stfs;
            int rc = statvfs(datadestdir.c_str(), &stfs);
            if(rc == -1) {
                std::ostringstream ss;
                ss << "Error running statvfs on " << datadestdir
                    << ": " << strerror(errno);
                throw Error(ss);
            }

            // Calculate how full the file system would be if we were to
            // add this file to it.
            fsblkcnt_t fsblocks = filesize / stfs.f_bsize;
            double percent_free = 100.00 * ((double)(stfs.f_bavail - fsblocks) / (double)stfs.f_blocks);
            if(100.00 - percent_free >= percent_full) {
                is_disk_full = true;
                std::ostringstream ss;
                ss << "Not enough space to deserialise " << filename
                    << " (" << filesize << " bytes) - would leave only "
                    << percent_free << "% free space";
                throw Error(ss);
            }


            // All good?  Open file.  All non-lrls go into the data directory.
            if(datadestdir.empty()) {
                throw Error("Stream contains files for data directory before data dir is known");
            }

            direct = false;

            if (manifest_it != manifest_map.end() && manifest_it->second.get_type() == FileInfo::BERKDB_FILE)
                direct = 1;

            std::string outfilename(datadestdir + "/" + filename);
            fullpath = outfilename;

            //of_ptr = output_file(outfilename, false, direct);
            extracted_files.insert(outfilename);
        }


        // Determine buffer size to read this data in.  If there is a known
        // page size then use that so that we can verify checksums as we go.
        size_t pagesize = 0;
        bool checksums = false;
        file_is_sparse = false;
        if(manifest_it != manifest_map.end()) {
            pagesize = manifest_it->second.get_pagesize();
            checksums = manifest_it->second.get_checksums();
            file_is_sparse = manifest_it->second.get_sparse();
        }
        if(pagesize == 0) {
            pagesize = 4096;
        }
        size_t bufsize = pagesize;

        if (!file_is_sparse) {
            while((bufsize << 1) <= MAX_BUF_SIZE) {
                bufsize <<= 1;
            }
        }


        uint8_t *buf;
#if defined _HP_SOURCE || defined _SUN_SOURCE
        buf = (uint8_t*) memalign(512, bufsize);
#else
        if (posix_memalign((void**) &buf, 512, bufsize))
            throw Error("Failed to allocate output buffer");
#endif
        RIIA_malloc free_guard(buf);

        // Read the tar data in and write it out
        unsigned long long bytesleft = filesize;
        unsigned long long pageno = 0;

        // Recheck the filesystem periodically while writing
        unsigned long long recheck_count = FS_PERIODIC_CHECK;

        bool checksum_failure = false;

        unsigned long long readbytes = 0;

        if (!is_text) {
            do_write(filename, datadestdir, bytesleft, bufsize,
                file_is_sparse, padding_bytes, percent_full, direct,
                deserialization_threads);
        } else {

/*
            check_remaining_space(datadestdir, percent_full, filename,
                filesize);
*/

            while(bytesleft > 0)
            {
                readbytes = bytesleft;

                if(bytesleft > bufsize)
                    readbytes = bufsize;

                if (readbytes > bytesleft)
                    readbytes = bytesleft;

                if(readall(0, &buf[0], readbytes) != readbytes)
                {
                    std::ostringstream ss;

                    ss << "Error reading " << readbytes << " bytes for file "
                        << filename << " after "
                        << (filesize - bytesleft) << " bytes, with "
                        << bytesleft << " bytes left to read:"
                        << errno << " " << strerror(errno);
                    throw Error(ss);
                }


                text.append((char*) &buf[0], readbytes);

                bytesleft -= readbytes;
                recheck_count -= readbytes;
            }
            // Read and discard the null padding
            if(padding_bytes) {
                if(readall(0, &buf[0], padding_bytes) != padding_bytes) {
                    std::ostringstream ss;
    
                    ss << "Error reading padding after " << filename
                        << ": " << errno << " " << strerror(errno);
                    throw Error(ss);
                }
            }
        }

        std::clog << "x " << filename << " size=" << filesize
                  << " pagesize=" << pagesize;

        if (file_is_sparse)
           std::clog << " SPARSE ";
        else
           std::clog << " not sparse ";

        std::clog << std::endl;

        if(checksum_failure && !force_mode) {
            std::ostringstream ss;
            ss << "Checksum verification failures in " << filename;
            throw Error(ss);
        }

        /* Restore the permissions. */
        uid_t uid = (uid_t)strtol(head.h.uid, NULL, 8);
        gid_t gid = (gid_t)strtol(head.h.gid, NULL, 8);
        mode_t modes = (mode_t)strtol(head.h.mode, NULL, 8);
        if (chown(fullpath.c_str(), uid, gid)==-1)
		perror(fullpath.c_str());
        if (chmod(fullpath.c_str(), modes)==-1)
		perror(fullpath.c_str());

        if (is_manifest) {
            process_manifest(text, manifest_map, run_full_recovery, origlrlname, options);
        } else if (is_lrl) {
            std::ostringstream lrldata;
            process_lrl(lrldata, filename, text, strip_cluster_info,
                        strip_consumer_info, datadestdir, lrldestdir, dbname,
                        table_set);
            if (!datadestdir.empty()) {
                make_dirs(datadestdir);
                if (empty_dir(datadestdir)) {
                    std::string dbtxndir(datadestdir + "/" + dbname + ".txn");
                    make_dirs(dbtxndir);
                    dbtxndir = datadestdir + "/" + "logs";
                    make_dirs(dbtxndir);
                } else if (check_dest_dir(datadestdir)) {
                    /* Remove old log files.  This used to remove all files in
                       the directory, which can be problematic if hi. */
                    if (!legacy_mode) {
                        // remove files in the txn directory
                        std::string dbtxndir(datadestdir + "/" + dbname +
                                             ".txn");
                        make_dirs(dbtxndir);
                        if (!incr_mode)
                            remove_all_old_files(dbtxndir);
                        dbtxndir = datadestdir + "/" + "logs";
                        make_dirs(dbtxndir);
                        if (!incr_mode)
                            remove_all_old_files(dbtxndir);
                    }
                }
            }
            // If the lrl was renames, restore the orginal name
            std::string outfilename;
            if (origlrlname != "")
                outfilename = (lrldestdir + "/" + origlrlname);
            else
                outfilename = (lrldestdir + "/" + filename);
            of_ptr = output_file(outfilename, true, false);
            *of_ptr << lrldata.str();
            if (main_lrl_file.empty()) {
                main_lrl_file = outfilename;
            }
            extracted_files.insert(outfilename);
        }
    }

    // If we never inited the txn dir then we must never have had a valid lrl;
    // fail in this case.
    if(!inited_txn_dir) {
        throw Error("No valid lrl file seen or txn dir not inited");
    }

    // Run full recovery
    if(run_full_recovery) {
        std::ostringstream cmdss;

        if (run_with_done_file)
           cmdss << "comdb2_stdout_redir ";
        
        cmdss<< comdb2_task << " " 
             << dbname << " -lrl "
             << main_lrl_file 
             << " -fullrecovery";

        for (int i = 0; i < options.size(); i++) {
            cmdss << " " << options[i];
        }

        std::clog << "Running full recovery: " << cmdss.str() << std::endl;

        errno = 0;
        int rc = std::system(cmdss.str().c_str());
        if(rc) {
            std::ostringstream ss;
            ss << "Full recovery command '" << cmdss.str() << "' failed rc "
                << rc << " errno " << errno << std::endl;
            throw Error(ss);
        }
        std::clog << "Full recovery successful" << std::endl;
    }




    // Remove the copylock file
    if(!copylock_file.empty()) {
        unlink(copylock_file.c_str());
    }

    // If it's a nonames db, remove the txn directory
    if (empty_dir(datadestdir + "/" + dbname + ".txn"))
        rmdir((datadestdir + "/" + dbname + ".txn").c_str());

    std::string fluff_file;
    fluff_file = datadestdir + "/FLUFF";
    unlink(fluff_file.c_str());
}

void deserialize_file(const std::string &filename, off_t filesize, bool write_file, bool dryrun, bool do_direct_io) {
    /* must be a multiple of 512! */
#define DESERIALIZE_BUFSIZE 1024*1024
    static char buf[DESERIALIZE_BUFSIZE];
    int rb; // read bytes
    const int bufsz = DESERIALIZE_BUFSIZE;

    std::unique_ptr<fdostream> out;
    if (write_file && !dryrun)
        out = output_file(filename, false, do_direct_io);

    for (int i = 0; i < filesize / sizeof(buf); i++) {
        rb = readall(0, buf, sizeof(buf));
        if (rb != sizeof(buf)) {
            std::ostringstream ss;
            ss << "Unexpected eof reading " << filename;
            throw Error(ss.str());
        }
        if (write_file && !dryrun)
            out->write(buf, bufsz);
        else if (dryrun) {
            std::cout << filename << ": " << bufsz << " bytes written" << std::endl;
        }
    }
    if (filesize % bufsz) {
        int remnant = filesize % bufsz;
        rb = readall(0, buf, remnant);
        if (rb != remnant) {
            std::ostringstream ss;
            ss << "Unexpected eof (last buffer) for " << filename;
            throw Error(ss.str());
        }
        if (write_file && !dryrun)
            out->write(buf, remnant);
        else if (dryrun)
            std::cout << filename << ": " << remnant << " bytes written (remnant)" << std::endl;
        if (remnant % 512) {
            remnant = 512 - (remnant % 512);
            rb = readall(0, buf, remnant);
            if (rb != remnant) {
                std::ostringstream ss;
                ss << "Unexpected eof (padding) for " << filename;
                throw Error(ss.str());
            }
        }
    }
}

void restore_partials(
    const std::string &lrlpath, const std::string& comdb2_task, bool run_full_recovery, bool do_direct_io, bool dryrun
// On stdio is a tarball containing a partial file. Apply it against the current
// set of database files in the directory given by the lrl file.
) {
    std::string dbname;
    std::string dbdir;
    std::list<std::string> support_files;
    std::set<std::string> table_names;
    std::set<std::string> queue_names;
    bool nonames;
    bool has_cluster_info;
    tar_block_header hdr;
    bool is_manifest = false;
    unsigned long long filesize;
    uint8_t zeroblock[512] = {0};
    int64_t manifest_size;

    // settings encoded in manifest
    std::map<std::string, std::pair<FileInfo, std::vector<uint32_t>>> updated_files;
    std::map<std::string, FileInfo> new_files;
    std::set<std::string> deleted_files;
    std::vector<std::string> file_order;
    std::vector<std::string> options;
    std::string manifest_sha;

    parse_lrl_file(lrlpath, &dbname, &dbdir, &support_files, &table_names, &queue_names, &nonames, &has_cluster_info);

    if (!dbdir.empty() && !dbname.empty()) {
        nonames = check_usenames(dbname, dbdir, nonames);
    }

    // The first thing we need to do is to delete all the logs - we 
    // don't want recovery run on partial logs
    std::string logpath;
    if (nonames)
        logpath = dbdir + "/logs";
    else
        logpath = dbdir + "/" + dbname + ".txn";

    std::cout << "start restore, datapath " << dbdir << " logpath " << logpath << std::endl;

    std::list<std::string> logfiles;
    listdir(logfiles, logpath);
    for (std::list<std::string>::const_iterator it = logfiles.begin();
            it != logfiles.end(); ++it) {
        if (it->compare(0, 4, "log.") == 0 && it->length() == 14) {
            std::string logfile = logpath + "/" + *it;
            unlink(logfile.c_str());
        }
    }


    for (;;) {
        ssize_t rb = readall(0, &hdr, sizeof(hdr));
        if (rb != sizeof(hdr)) {
            std::ostringstream ss;
            ss << "huh? read " << rb << "bytes";
            throw Error(ss.str());
        }

        if (std::memcmp(hdr.c, zeroblock, 512) == 0)
            break;

        if (hdr.h.filename[sizeof(hdr.h.filename) - 1] != '\0') {
            throw Error("Bad block: filename is not null terminated");
        }
        if(!read_octal_ull(hdr.h.size, sizeof(hdr.h.size), filesize)) {
            read_octal_ull(hdr.h.size, sizeof(hdr.h.size), filesize);
            std::cout << hdr.h.size << std::endl;
            throw Error("Bad block: bad size ");
        }
        std::string filename(hdr.h.filename);
        if (filename == "INCR_MANIFEST") {
            std::string manifest_text = read_incr_manifest(filesize);
            if(!process_incr_manifest(manifest_text, dbdir,
                        updated_files, new_files, deleted_files,
                        file_order, options, manifest_sha)){
                // Failed to read a coherent manifest
                std::ostringstream ss;
                ss << "Error reading manifest";
                throw Error(ss);
            }

            // Clear the logfile folder
            clear_log_folder(dbdir, dbname);

            // delete the deleted files
            for (std::set<std::string>::const_iterator ii = deleted_files.begin();
                    ii != deleted_files.end(); ++ii) {
                // std::cout << "deleted " << dbdir + "/" + *ii << std::endl;
                std::ostringstream ss;
                ss << dbdir << "/" << filename;
                int rc;
                if (!dryrun)
                    rc = unlink(ss.str().c_str());
                else
                    std::cout << "unlink " << ss.str() << " rc " << rc << std::endl;
            }
            for (std::map<std::string, FileInfo>::const_iterator ii = new_files.begin();
                    ii != new_files.end(); ++ii) {
                std::cout << "new " << (dbdir + "/" + (*ii).second.get_filename()) << std::endl;
            }
            for (std::map<std::string, std::pair<FileInfo, std::vector<uint32_t>>>::const_iterator ii = updated_files.begin();
                    ii != updated_files.end(); ++ii) {
                std::cout << "updated " << dbdir + "/" + ii->first << std::endl;
            }
            continue;
        }

        bool write_file = false;

        if (filename.substr(filename.length() - 5, 5) == ".data") {
            unpack_incr_data(file_order, updated_files, dbdir, dryrun);
            write_file = false; 
        }
        else if (new_files.find(filename) != new_files.end()) {
            write_file = true;
        }
        else if (deleted_files.find(filename) != deleted_files.end()) {
            std::cout << "found a deleted file in tarball?" << std::endl;
            write_file = false;
        }
        else if (updated_files.find(filename) != updated_files.end()) {
            std::cout << "update " << filename << std::endl;
            write_file = true;
        }
        else {
            std::cout << "support-file " << filename << std::endl;
            write_file = true;
        }

        if (!write_file)
            continue;

        std::ostringstream ss;
        ss << dbdir << "/" << filename;
        filename = ss.str();
        if (write_file)
            deserialize_file(filename, filesize, write_file, dryrun, do_direct_io);
    }

    if(run_full_recovery) {
        std::ostringstream cmdss;

        cmdss<< comdb2_task << " " 
             << dbname << " -lrl "
             << lrlpath 
             << " -fullrecovery";

        for (int i = 0; i < options.size(); i++) {
            cmdss << " " << options[i];
        }

        std::clog << "Running full recovery: " << cmdss.str() << std::endl;

        errno = 0;
        int rc = std::system(cmdss.str().c_str());
        if(rc != 0) {
            std::ostringstream ss;
            ss << "Full recovery command '" << cmdss.str() << "' failed rc "
                << rc << " errno " << errno << std::endl;
            throw Error(ss);
        }
        std::clog << "Full recovery successful" << std::endl;
    }
}
