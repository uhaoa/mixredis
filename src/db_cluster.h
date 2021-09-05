/*
* Copyright (C) 2019  Giuseppe Fabio Nicotra <artix2 at gmail dot com>
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Affero General Public License as published
* by the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU Affero General Public License for more details.
*
* You should have received a copy of the GNU Affero General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#ifndef __REDIS_CLUSTER_PROXY_CLUSTER_H__
#define __REDIS_CLUSTER_PROXY_CLUSTER_H__

#include "server.h"
#include "sds.h"
#include "adlist.h"
#include "rax.h"
#include "config.h"
#include <hiredis.h>
#include <time.h>


#define DB_CLUSTER_RECONFIG_ERR        -1
#define DB_CLUSTER_RECONFIG_ENDED      0
#define DB_CLUSTER_RECONFIG_WAIT       1
#define DB_CLUSTER_RECONFIG_STARTED    2
#define getClusterNodeContext(node) (node->connection->context)
#define isClusterNodeConnected(node) (node->connection->connected)
enum cluserType {
	CLUSER_REDIS,
	CLUSER_TENDIS,
};
#define CLUSTER_TYPE_FMT " cluster type:%s "

#define ERROR_DB_CLUSTER_RECONFIG \
    "-CLUSTERDOWN Failed to fetch db cluster configuration"

#define ERROR_NO_DB_NODE "Failed to get db node for request"

struct dbCluster;
struct dbNode;

typedef struct dbClusterConnection {
	redisContext *context;
	list *requests_to_send;
	list *requests_pending;
	int connected;
	int has_read_handler;
	struct dbNode *node;
} dbClusterConnection;

typedef struct dbNode {
	dbClusterConnection *connection;
	struct dbCluster *cluster;
	sds ip;
	int port;
	sds name;
	int flags;
	sds replicate;  /* Master ID if node is a replica */
	int is_replica;
	int *slots;
	int slots_count;
	int replicas_count; /* Value is always -1 until 'PROXY CLUSTER' command
						* counts all replicas and stores the result in
						* `replicas_count`, that is actually used as a
						* cache. */
	sds *migrating; /* An array of sds where even strings are slots and odd
					* strings are the destination node IDs. */
	sds *importing; /* An array of sds where even strings are slots and odd
					* strings are the source node IDs. */
	int migrating_count; /* Length of the migrating array (migrating slots*2) */
	int importing_count; /* Length of the importing array (importing slots*2) */
	struct dbNode *duplicated_from;
} dbNode;

enum dbRequestType {
	REQUEST_READ,
	REQUEST_WRITE,
};

typedef struct dbRequest {
	uint64_t id;
	int request_type;

	robj *keyobj;					/* REQUEST_WRITE & REQUEST_READ */
	sds buffer;						/* REQUEST_WRITE & REQUEST_READ */

	uint64_t dbid;					/* REQUEST_WRITE */
	size_t object_size;				/* REQUEST_WRITE */

	robj *argv[5];					/* REQUEST_READ */
	robj* value_obj;				/* REQUEST_READ */

	dbNode *node;
	size_t written;
	int has_write_handler;
	int need_reprocessing;
	
	uint64_t client_id;            /* Client incremental unique ID. */
	listNode *requests_to_send_lnode; /* Pointer to node in
									  * redisClusterConnection->
									  *  requests_to_send list */
	listNode *requests_pending_lnode; /* Pointer to node in
									  * redisClusterConnection->
									  * requests_pending list */

	uint64_t param_ex;
} dbRequest;

typedef struct dbCluster {
	/*int thread_id;*/
	list *nodes;
	rax  *slots_map;
	rax  *nodes_by_name;
	list *master_names;
	int masters_count;
	int replicas_count;
	dbClusterEntryPoint *entry_point;
	rax  *requests_to_reprocess;
	int is_updating;
	int update_required;
	int broken;
	struct dbCluster *duplicated_from;
	list *duplicates;
	size_t db_free_memory; 
} dbCluster;

dbCluster *createDbCluster();
int resetDbCluster(dbCluster *cluster);
void freeDbCluster(dbCluster *cluster);
int fetchDbClusterConfiguration(dbCluster *cluster,
	dbClusterEntryPoint* entry_points,
	int entry_points_count);
dbRequest *createDbRequest(int request_type);
void freeDbRequest(dbRequest *req);
int postDbRequest(dbRequest *req);
redisContext *dbNodeConnect(dbNode *node);
void dbNodeDisconnect(dbNode *node);
dbNode *searchNodeBySlot(dbCluster *cluster, int slot);
dbNode *getNodeByKey(dbCluster *cluster, char *key, int keylen);
dbNode *getNodeByName(dbCluster *cluster, const char *name);
dbNode *getFirstMappedNode(dbCluster *cluster);
list *dbClusterGetMasterNames(dbCluster *cluster);
int updateDbCluster(dbCluster *cluster);
void clusterAddRequestToReprocess(dbCluster *cluster, void *r);
void clusterRemoveRequestToReprocess(dbCluster *cluster, void *r);
dbClusterConnection *createDbClusterConnection(void);
void freeDbClusterConnection(dbClusterConnection *conn);
dbClusterEntryPoint *copyEntryPoint(dbClusterEntryPoint *source);
void freeEntryPoints(dbClusterEntryPoint *entry_points, int count);
void initDbCluster(); 

#endif /* __REDIS_CLUSTER_PROXY_CLUSTER_H__ */
