#ifndef INCLUDED_COMDB2BUF_H
#define INCLUDED_COMDB2BUF_H

#include <sysutil_ident.h>
SYSUTIL_IDENT_RCSID(comdb2buf_h,"$Id$ $CSID$")
SYSUTIL_PRAGMA_ONCE

/* Define this to check/allocate for buffer on every read/write.
 * Undef to do it once in cdb2buf_setbufsize */
#undef CDB2BUF_DELAY_MALLOC

/* Define this to call through function pointers. */
/* Undefine to directly call the functions */
#undef CDB2BUF_RWFUNCS

/* Define this to enable ungetc */
/* Undef to remove check and we can skip checking on every read */
#undef CDB2BUF_UNGETC

#if defined __cplusplus
extern "C"
{
#endif

typedef struct comdb2buf COMDB2BUF;

enum CDB2BUF_FLAGS
{
    CDB2BUF_WRITE_LINE=1,         /* FLUSH OUTPUT ON \n */
    CDB2BUF_DEBUG_LAST_LINE=2,    /* STORE LAST LINE IN/OUT IN DEBUG BUFFER */
    CDB2BUF_NO_CLOSE_FD = 4,      /* cdb2buf_close() will not close the underlying fd */
    CDB2BUF_NO_FLUSH = 8,
    CDB2BUF_NO_BLOCK = 16         /* adjust read/write calls to write on non-blocking socket */
};

/* retrieve underlying fd */
int cdb2buf_fileno(COMDB2BUF *);

/* open COMDB2BUF for file descriptor.  returns COMDB2BUF handle or 0 if error.*/
COMDB2BUF *cdb2buf_open(int fd, int flags);

/* flush output, close fd, and free COMDB2BUF. 0==success */
int cdb2buf_close(COMDB2BUF *);

/* flush output, close fd, and free COMDB2BUF.*/
int cdb2buf_free(COMDB2BUF *);

/* flush output.  returns # of bytes written or <0 for error */
int cdb2buf_flush(COMDB2BUF *);

/* fwrite to COMDB2BUF. returns # of items written or <0 for error */
int cdb2buf_fwrite(char *ptr, int size, int nitems, COMDB2BUF *);

/* returns string len (not including \0) and string. <0 means err*/
int cdb2buf_gets(char *out, int lout, COMDB2BUF *);

/* fread from COMDB2BUF. returns # of items read or <0 for error */
int cdb2buf_fread(char *ptr, int size, int nitems, COMDB2BUF *);

/* returns # of bytes written or <0 for err*/
int cdb2buf_printf(COMDB2BUF *, char *fmt, ...);

/* set up poll timeout on file descriptor*/
void cdb2buf_settimeout(COMDB2BUF *, int readtimeout, int writetimeout);

/* sets buffer size. this does NOT flush for you. if you
   change the buffer size, it will lose any buffered writes.
   default buffer size is 1K */
void cdb2buf_setbufsize(COMDB2BUF *, unsigned int size);

/* write to COMDB2BUF. returns number of bytes written or <0 for error */
int cdb2buf_write(char *ptr, int nbytes, COMDB2BUF *);

/* returns # of bytes written or <0 for err*/
int cdb2buf_printfx(COMDB2BUF *, char *buf, int lbuf, char *fmt, ...);

/* set flags on an COMDB2BUF after opening */
void cdb2buf_setflags(COMDB2BUF *, int flags);


/* Parts of the sbuf not used by Comdb2 API */

#if 0
/* put character. returns # of bytes written (always 1) or <0 for err */
int cdb2buf_putc(COMDB2BUF *, char c);

/* put \0 terminated string. returns # of bytes written or <0 for err */
int cdb2buf_puts(COMDB2BUF *, char *string);

/* get character. returns character read */
int cdb2buf_getc(COMDB2BUF *);

/* close fd and free COMDB2BUF. 0==success */
int cdb2buf_close_noflush(COMDB2BUF *);

#ifdef CDB2BUF_RWFUNCS
/* set custom read/write routines */
typedef int (*cdb2buf_readfn)(COMDB2BUF *, char *buf, int nbytes);
typedef int (*cdb2buf_writefn)(COMDB2BUF *, const char *buf, int nbytes);
void cdb2buf_setrw(COMDB2BUF *, cdb2buf_readfn read, cdb2buf_writefn write);
void cdb2buf_setr(COMDB2BUF *, cdb2buf_readfn read);
void cdb2buf_setw(COMDB2BUF *, cdb2buf_writefn write);
cdb2buf_readfn cdb2buf_getr(COMDB2BUF *);
cdb2buf_writefn cdb2buf_getw(COMDB2BUF *);
#endif

/* return last line read*/
char *cdb2buf_dbgin(COMDB2BUF *);

/* return last line written*/
char *cdb2buf_dbgout(COMDB2BUF *);

/* set the userptr associated with this sbuf - use this for whatever your
 * application desires. */
void cdb2buf_setuserptr(COMDB2BUF *, void *userptr);

/* get the user pointer associated with this sbuf */
void *cdb2buf_getuserptr(COMDB2BUF *);

#ifdef CDB2BUF_UNGETC
int cdb2buf_ungetc(char c, COMDB2BUF *);
int cdb2buf_eof(COMDB2BUF *);
#endif

#endif

#if defined __cplusplus
}
#endif

#endif /* INCLUDED_COMDB2BUF_H */
