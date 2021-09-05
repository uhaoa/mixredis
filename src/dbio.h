#ifndef __DBIO_H
#define __DBIO_H
#include "db_cluster.h"
/* Exported API */
void dbioInit(void);
void asyncPostDbRequest(dbRequest * req);
void asyncPostDbResponse(dbRequest * req);
void dbioKillThreads(void);
void processDbResponse(struct aeEventLoop* eventLoop, long long id, void* clientData, int mask);
int tryReadEmptyKeys(client *c);
dbRequest* fetchDbRequestObject(int request_type);
void recycleDbRequestObject(dbRequest* req);
#endif
