#ifndef _DB_LSN_H_
#define _DB_LSN_H_

struct __db_lsn;	typedef struct __db_lsn DB_LSN;

struct __db_lsn {
	u_int32_t	file;		/* File ID. */
	u_int32_t	offset;		/* File offset. */
};

#endif
