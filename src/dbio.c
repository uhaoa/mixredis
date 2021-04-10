#include "server.h"
#include <sys/eventfd.h>
#include "dbio.h"
#include "assert.h"
#include "cluster.h"
#include "atomicvar.h"

static pthread_t dbio_thread;
static list *dbio_req_list;
static list *dbio_resp_list;
static pthread_mutex_t dbio_mutex; 
static int event_fd = 0;
static size_t already_wakeup = 0;

int dbioServerCron(struct aeEventLoop *eventLoop, long long id, void *clientData, int mask);
void *dbioMain(void *arg); 
void removeEvictKey(robj *keyobj, int dbid, int force); 

/* Initialize the background system, spawning the thread. */
void dbioInit(void) {
    pthread_t thread;
	pthread_mutex_init(&dbio_mutex, NULL);
	dbio_req_list = listCreate();
	dbio_resp_list = listCreate();
	event_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK); 
	if (aeCreateFileEvent(server.db_el, event_fd, AE_READABLE, dbioServerCron, NULL) == AE_ERR) {
		serverDbLog(LL_WARNING, "Fatal: aeCreateFileEvent event_fd.");
		exit(1);
	}
	if (pthread_create(&thread, NULL,  dbioMain, NULL) != 0) {
		serverDbLog(LL_WARNING, "Fatal: Can't initialize Background Jobs.");
		exit(1);
	}
	dbio_thread = thread;
}

void dbioWakeup()
{
	size_t wakeup;
	atomicGet(already_wakeup, wakeup); 
	if (wakeup == 1)
		return; 
	atomicSet(already_wakeup, 1);
	int64_t data = 1;
	ssize_t ret = write(event_fd, &data, sizeof(data));
	UNUSED(ret); 
}

void *dbioMain(void *arg) {
	UNUSED(arg);
	server.db_el->stop = 0;
	while (!server.db_el->stop) {
		aeProcessEvents(server.db_el, AE_ALL_EVENTS |
			AE_CALL_BEFORE_SLEEP |
			AE_CALL_AFTER_SLEEP);
	}
	return NULL;
}

int dbioServerCron(struct aeEventLoop *eventLoop, long long id, void *clientData , int mask) {
	UNUSED(eventLoop);
	UNUSED(id);
	UNUSED(clientData);
	UNUSED(mask);

	atomicSet(already_wakeup, 0);

	static char buf[1024] = {0};
	while (1) {
		int ret = read(event_fd, buf, sizeof(buf));
		if (ret == -1 || (size_t)ret < sizeof(buf))
			break;
	}

	pthread_mutex_lock(&dbio_mutex);
	while (1)
	{
		if (listLength(dbio_req_list) == 0) {
			break;
		}
		listNode *ln = listFirst(dbio_req_list);
		assert(ln);
		dbRequest *dbreq = listNodeValue(ln);
		postDbRequest(dbreq);
		listDelNode(dbio_req_list, ln);
	}
	pthread_mutex_unlock(&dbio_mutex);

	return 0; 
}

void asyncPostDbRequest(dbRequest * req)
{
	assert(req);

	pthread_mutex_lock(&dbio_mutex);
	listAddNodeTail(dbio_req_list, req);
	pthread_mutex_unlock(&dbio_mutex);

	dbioWakeup();
}

void asyncPostDbResponse(dbRequest * res) 
{
	assert(res);
	pthread_mutex_lock(&dbio_mutex);
	listAddNodeTail(dbio_resp_list, res);
	pthread_mutex_unlock(&dbio_mutex);
}

int tryReadEmptyKeys(client *c)
{
	int *keyindex, numkeys, emptynums = 0;
	struct redisCommand *cmd = lookupCommand(c->argv[0]->ptr);	
	if (cmd == NULL)
		return 0; 
	if (cmd->proc == restoreCommand && !nodeIsMaster(server.cluster->myself)) {
		return 0; 
	}
	keyindex = getKeysFromCommand(cmd, c->argv, c->argc, &numkeys);
	for (int i = 0; i < numkeys; i++) {
		robj *thiskey = c->argv[keyindex[i]];
		robj *obj = lookupKeyRead(c->db, thiskey); 
		if (obj == shared.emptyvalue) {
			emptynums++; 
			if (nodeIsMaster(server.cluster->myself)) {
				dbRequest *dbreq = createDbRequest(REQUEST_READ);
				dbreq->dbid = c->db->id;
				dbreq->client_id = c->id;
				dbreq->keyobj = createStringObject(thiskey->ptr, sdslen(thiskey->ptr));
				dbreq->buffer = sdscatprintf(sdsempty(), "*2\r\n$3\r\nget\r\n$%i\r\n%s\r\n", sdslen(thiskey->ptr), thiskey->ptr);
				asyncPostDbRequest(dbreq);
			}
		}
	}
	if (emptynums > 0) {
		if (nodeIsMaster(server.cluster->myself)) {
			c->db_load_req_num = emptynums;
			blockClient(c, BLOCKED_DB_LOAD);
			return 1; 
		}
		else {
			// 临时处理
			addReply(c, shared.err);
			return 2;
		}
	}
	return 0; 
}

void processDbResponse()
{
	while (1)
	{
		pthread_mutex_lock(&dbio_mutex);
		if (listLength(dbio_resp_list) == 0) {
			pthread_mutex_unlock(&dbio_mutex);
			break;
		}
		dbRequest *req = NULL;
		listNode *ln = listFirst(dbio_resp_list);
		assert(ln);
		req = listNodeValue(ln);
		listDelNode(dbio_resp_list, ln);
		pthread_mutex_unlock(&dbio_mutex);

		if (req->request_type == REQUEST_WRITE) {
			removeEvictKey(req->keyobj, req->dbid, 0);
		}
		else if(req->request_type == REQUEST_READ) {
			redisDb *db = server.db + req->dbid;
			robj* obj = lookupKeyWrite(db,req->keyobj);
			if (obj && obj == shared.emptyvalue && req->value_obj) {
				/* remove the old key.*/
				dbDelete(db, req->keyobj); 
				/* create new.*/
				dbAdd(db, req->keyobj, req->value_obj); 				
				
				struct redisCommand* cmd = lookupCommand(req->argv[0]->ptr); 
				assert(cmd);
				
				/* feed to aof. */
				if (server.aof_state != AOF_OFF)
					feedAppendOnlyFile(cmd, req->dbid, req->argv, 5);
				/* feed to slave.*/
				replicationFeedSlaves(server.slaves, req->dbid, req->argv, 5); 

				req->value_obj = NULL;
			}
			else {
				serverDbLog(LL_WARNING, "read response error."); 
			}
			client* c = lookupClientByID(req->client_id);
			if (c && (c->flags & CLIENT_BLOCKED) && c->btype == BLOCKED_DB_LOAD) {
				unblockClient(c);
			}
		}
		else {
			assert(0); 
		}
		freeDbRequest(req); 
	}
}


void dbioKillThreads(void) {
    int err;

	if (dbio_thread && pthread_cancel(dbio_thread) == 0) {
		if ((err = pthread_join(dbio_thread, NULL)) != 0) {
			serverDbLog(LL_WARNING,
				"dbio thread for job type can be joined: %s", strerror(err));
		}
		else {
			serverDbLog(LL_WARNING,
				"dbio thread for job type terminated");
		}
	}
}
