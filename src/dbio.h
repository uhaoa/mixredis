#ifndef __DBIO_H
#define __DBIO_H
#include "db_cluster.h"
/* Exported API */
void dbioInit(void);
void asyncPostDbRequest(dbRequest * req);
void asyncPostDbResponse(dbRequest * req);
void dbioKillThreads(void);
void processDbResponse(); 
int tryReadEmptyKeys(client *c);

#endif
