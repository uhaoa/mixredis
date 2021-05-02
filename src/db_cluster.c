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
#include "db_cluster.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <hiredis.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include "anet.h"
#include "zmalloc.h"
//#include "logger.h"
#include "config.h"
#include "assert.h" /* Use proxy's assert */
#include <signal.h>
#include "cluster.h"

#define CLUSTER_NODE_KEEPALIVE_INTERVAL 15	
#define QUEUE_TYPE_SENDING                  1
#define QUEUE_TYPE_PENDING                  2
#define ERROR_DB_NODE_DISCONNECTED "db cluster node disconnected: "
#define ERROR_DB_CLUSTER_READ_FAIL "Failed to read from db cluster"

#define addObjectToList(obj, listowner, listname, success) do {\
    if (listAddNodeTail(listowner->listname, obj) == NULL) {\
        if (success != NULL) *success = 0;\
        obj->listname ## _lnode = NULL;\
        break;\
    }\
    if (success != NULL) *success = 1;\
    obj->listname ## _lnode = listLast(listowner->listname);\
} while (0)
#define removeObjectFromList(obj, listowner, listname) do {\
    if (obj->listname ## _lnode != NULL) {\
        listDelNode(listowner->listname, obj->listname ## _lnode);\
        obj->listname ## _lnode = NULL;\
    }\
} while (0)
#define enqueueRequestToSend(req) (enqueueRequest(req, QUEUE_TYPE_SENDING))
#define dequeueRequestToSend(req) (dequeueRequest(req, QUEUE_TYPE_SENDING))
#define enqueuePendingRequest(req) (enqueueRequest(req, QUEUE_TYPE_PENDING))
#define dequeuePendingRequest(req) (dequeueRequest(req, QUEUE_TYPE_PENDING))
#define getFirstRequestToSend(node, isempty) \
    (getFirstQueuedRequest(node->connection->requests_to_send,\
     isempty))
#define getFirstRequestPending(node, isempty) \
    (getFirstQueuedRequest(node->connection->requests_pending,\
     isempty))

#define REQID_PRINTF_FMT "{%" PRId64 ":%" PRId64 ":%" PRId32  "}"
#define REQID_PRINTF_ARG(r) r->id ,r->client_id , r->request_type

/* Forward declarations. */

static void freeDbNode(dbNode *node);
static int sendRequestToDbCluster(dbRequest *req, sds *errmsg);
static void freeDbRequestList(list *request_list); 
static void writeToClusterHandler(aeEventLoop *el, int fd, void *privdata, int mask); 
void removeEvictKey(robj *keyobj, int dbid, int force); 
/* Hiredis helpers */
int processItem(redisReader *r);
/* Utils */

uint16_t crc16(const char *buf, int len);

/* -----------------------------------------------------------------------------
* Key space handling
* -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
* as the least significant 14 bits of the crc16 of the key.
*
* However if the key contains the {...} pattern, only the part between
* { and } is hashed. This may be useful in the future to force certain
* keys to be in the same node (assuming no resharding is in progress). */
static unsigned int clusterKeyHashSlot(char *key, int keylen) {
	int s, e; /* start-end indexes of { and } */

	for (s = 0; s < keylen; s++)
		if (key[s] == '{') break;

	/* No '{' ? Hash the whole key. This is the base case. */
	if (s == keylen) return crc16(key, keylen) & 0x3FFF;

	/* '{' found? Check if we have the corresponding '}'. */
	for (e = s + 1; e < keylen; e++)
		if (key[e] == '}') break;

	/* No '}' or nothing between {} ? Hash the whole key. */
	if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

	/* If we are here there is both a { and a } on its right. Hash
	* what is in the middle between { and }. */
	return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

/* Cluster functions. */

dbClusterConnection *createDbClusterConnection(void) {
	dbClusterConnection *conn = zmalloc(sizeof(*conn));
	if (conn == NULL) return NULL;
	conn->context = NULL;
	conn->has_read_handler = 0;
	conn->connected = 0;
	conn->requests_pending = listCreate();
	if (conn->requests_pending == NULL) {
		zfree(conn);
		return NULL;
	}
	conn->requests_to_send = listCreate();
	if (conn->requests_to_send == NULL) {
		listRelease(conn->requests_pending);
		zfree(conn);
		return NULL;
	}
	conn->node = NULL;
	return conn;
}

void freeDbClusterConnection(dbClusterConnection *conn) {
	freeDbRequestList(conn->requests_pending);
	freeDbRequestList(conn->requests_to_send);
	redisContext *ctx = conn->context;
	if (ctx != NULL) redisFree(ctx);
	zfree(conn);
}

dbCluster *createDbCluster() {
	dbCluster *cluster = zcalloc(sizeof(*cluster));
	if (!cluster) return NULL;
	cluster->duplicated_from = NULL;
	cluster->duplicates = NULL;
	cluster->masters_count = 0;
	cluster->replicas_count = 0;
	cluster->entry_point = NULL;
	cluster->nodes = listCreate();
	if (cluster->nodes == NULL) {
		zfree(cluster);
		return NULL;
	}
	cluster->slots_map = raxNew();
	if (cluster->slots_map == NULL) {
		listRelease(cluster->nodes);
		zfree(cluster);
		return NULL;
	}
	cluster->nodes_by_name = raxNew();
	if (cluster->nodes_by_name == NULL) {
		freeDbCluster(cluster);
		return NULL;
	}
	cluster->requests_to_reprocess = raxNew();
	if (cluster->requests_to_reprocess == NULL) {
		freeDbCluster(cluster);
		return NULL;
	}
	/* The 'master_names' list is used by such commands as SCAN. It will
	* remain NULL until requested by the function dbClusterGetMasterNames,
	* so it doesn't use any memory if not needed. */
	cluster->master_names = NULL;
	cluster->is_updating = 0;
	cluster->update_required = 0;
	cluster->broken = 0;
	cluster->request_count = 0;
	return cluster;
}

static void clusterAddDbNode(dbCluster* cluster, dbNode *node) {
	listAddNodeTail(cluster->nodes, node);
	if (node->name) {
		raxInsert(cluster->nodes_by_name, (unsigned char*)node->name,
			strlen(node->name), node, NULL);
	}
}

static void onDbNodeDisconnection(dbNode *node) {
	UNUSED(node); 
}

static void freeDbNode(dbNode *node) {
	if (node == NULL) return;
	int i;
	if (node->connection != NULL) {
		onDbNodeDisconnection(node);
		freeDbClusterConnection(node->connection);
	}
	if (node->ip) sdsfree(node->ip);
	if (node->name) sdsfree(node->name);
	if (node->replicate) sdsfree(node->replicate);
	if (node->migrating != NULL) {
		for (i = 0; i < node->migrating_count; i++) sdsfree(node->migrating[i]);
		zfree(node->migrating);
	}
	if (node->importing != NULL) {
		for (i = 0; i < node->importing_count; i++) sdsfree(node->importing[i]);
		zfree(node->importing);
	}
	zfree(node->slots);
	zfree(node);
}

static void freeDbNodes(dbCluster *cluster) {
	if (!cluster || !cluster->nodes) return;
	listIter li;
	listNode *ln;
	listRewind(cluster->nodes, &li);
	while ((ln = listNext(&li)) != NULL) {
		dbNode *node = ln->value;
		freeDbNode(node);
	}
	listRelease(cluster->nodes);
}

int resetDbCluster(dbCluster *cluster) {
	cluster->masters_count = 0;
	cluster->replicas_count = 0;
	if (cluster->slots_map) raxFree(cluster->slots_map);
	if (cluster->nodes_by_name) raxFree(cluster->nodes_by_name);
	if (cluster->master_names) listRelease(cluster->master_names);
	freeDbNodes(cluster);
	cluster->slots_map = raxNew();
	cluster->nodes_by_name = raxNew();
	cluster->nodes = listCreate();
	if (!cluster->slots_map) return 0;
	if (!cluster->nodes) return 0;
	if (!cluster->nodes_by_name) return 0;
	return 1;
}

void freeDbCluster(dbCluster *cluster) {
	serverLog(LL_DEBUG, "Free shared cluster ");
	if (cluster->slots_map) raxFree(cluster->slots_map);
	if (cluster->nodes_by_name) raxFree(cluster->nodes_by_name);
	if (cluster->master_names) listRelease(cluster->master_names);
	freeDbNodes(cluster);
	if (cluster->requests_to_reprocess)
		raxFree(cluster->requests_to_reprocess);
	if (cluster->duplicates != NULL) {
		listIter li;
		listNode *ln;
		listRewind(cluster->duplicates, &li);
		while ((ln = listNext(&li))) {
			dbCluster *dup = ln->value;
			/* Set duplicated_from to NULL since it would point to a
			* freed cluster. */
			dup->duplicated_from = NULL;
			/* Also set duplicated_from to NULL into single nodes. */
			listIter nli;
			listNode *nln;
			listRewind(dup->nodes, &nli);
			while ((nln = listNext(&nli))) {
				dbNode *n = nln->value;
				n->duplicated_from = NULL;
			}
		}
		listRelease(cluster->duplicates);
	}
	if (cluster->duplicated_from != NULL) {
		list *parent_duplicates = cluster->duplicated_from->duplicates;
		if (parent_duplicates != NULL) {
			listNode *ln = listSearchKey(parent_duplicates, cluster);
			if (ln != NULL) listDelNode(parent_duplicates, ln);
		}
	}
	if (cluster->entry_point != NULL) {
		freeEntryPoints(cluster->entry_point, 1);
		zfree(cluster->entry_point);
	}
	zfree(cluster);
}

static dbNode *createDbNode(char *ip, int port, dbCluster *c) {
	dbNode *node = zcalloc(sizeof(*node));
	if (!node) return NULL;
	node->cluster = c;
	node->ip = sdsnew(ip);
	node->port = port;
	node->name = NULL;
	node->flags = 0;
	node->replicate = NULL;
	node->replicas_count = -1;
	node->slots = zmalloc(CLUSTER_SLOTS * sizeof(int));
	node->slots_count = 0;
	node->migrating = NULL;
	node->importing = NULL;
	node->migrating_count = 0;
	node->importing_count = 0;
	node->duplicated_from = NULL;
	node->connection = createDbClusterConnection();
	if (node->connection == NULL) {
		freeDbNode(node);
		return NULL;
	}
	node->connection->node = node;
	return node;
}

redisContext *dbNodeConnect(dbNode *node) {
	redisContext *ctx = getClusterNodeContext(node);
	if (ctx) {
		/*onDbClusterNodeDisconnection(node);*/
		redisFree(ctx);
		ctx = NULL;
	}
	serverLog(LL_DEBUG, "Connecting to node %s:%d", node->ip, node->port);
	ctx = redisConnectNonBlock(node->ip, node->port);
	if (ctx->err) {
		serverLog(LL_NOTICE, "Could not connect to Redis at %s:%d: %s",
			node->ip, node->port, ctx->errstr);
		redisFree(ctx);
		node->connection->context = NULL;
		return NULL;
	}
	/* Set aggressive KEEP_ALIVE socket option in the Redis context socket
	* in order to prevent timeouts caused by the execution of long
	* commands. At the same time this improves the detection of real
	* errors. */
	anetKeepAlive(NULL, ctx->fd, CLUSTER_NODE_KEEPALIVE_INTERVAL);
	node->connection->context = ctx;
	return ctx;
}

void dbNodeDisconnect(dbNode *node) {
	redisContext *ctx = getClusterNodeContext(node);
	if (ctx == NULL) return;
	serverLog(LL_DEBUG, "Disconnecting from node %s:%d", node->ip, node->port);
	onDbNodeDisconnection(node);
	redisFree(ctx);
	node->connection->context = NULL;
}

/* Map to slot into the cluster's radix tree map after converting the slot
* to bigendian. */
void mapSlot(dbCluster *cluster, int slot, dbNode *node) {
	uint32_t slot_be = htonl(slot);
	raxInsert(cluster->slots_map, (unsigned char *)&slot_be,
		sizeof(slot_be), node, NULL);
}

/* Consume Hiredis Reader buffer */
static void consumeRedisReaderBuffer(redisContext *ctx) {
	sdsrange(ctx->reader->buf, ctx->reader->pos, -1);
	ctx->reader->pos = 0;
	ctx->reader->len = sdslen(ctx->reader->buf);
}


int dbNodeLoadInfo(dbCluster *cluster, dbNode *node, list *friends,
	redisContext *ctx)
{
	int success = 1;
	redisReply *reply = NULL;
	if (ctx == NULL) {
		ctx = redisConnect(node->ip, node->port);
		if (ctx->err) {
			fprintf(stderr, "Could not connect to Redis at %s:%d: %s ",
				node->ip, node->port, ctx->errstr);
			redisFree(ctx);
			return 0;
		}
	}
	node->connection->context = ctx;
	node->connection->connected = 1;

	reply = redisCommand(ctx, "CLUSTER NODES");
	success = (reply != NULL);
	if (!success) goto cleanup;
	success = (reply->type != REDIS_REPLY_ERROR);
	if (!success) {
		fprintf(stderr, "Failed to retrieve db cluster configuration.\n");
		fprintf(stderr, "db cluster node %s:%d replied with error:\n%s\n",
			node->ip, node->port, reply->str);
		goto cleanup;
	}

	char *lines = reply->str, *p, *line;
	while ((p = strstr(lines, "\n")) != NULL) {
		*p = '\0';
		line = lines;
		lines = p + 1;
		char *name = NULL, *addr = NULL, *flags = NULL, *master_id = NULL;
		int i = 0;
		while ((p = strchr(line, ' ')) != NULL) {
			*p = '\0';
			char *token = line;
			line = p + 1;
			switch (i++) {
			case 0: name = token; break;
			case 1: addr = token; break;
			case 2: flags = token; break;
			case 3: master_id = token; break;
			}
			if (i == 8) break; // Slots
		}
		if (!flags) {
			fprintf(stderr, "Invalid CLUSTER NODES reply: missing flags.\n");
			success = 0;
			goto cleanup;
		}
		if (addr == NULL) {
			fprintf(stderr, "Invalid CLUSTER NODES reply: missing addr.\n");
			success = 0;
			goto cleanup;
		}
		int myself = (strstr(flags, "myself") != NULL);
		char *ip = NULL;
		int port = 0;
		char *paddr = strchr(addr, ':');
		if (paddr != NULL) {
			*paddr = '\0';
			ip = addr;
			addr = paddr + 1;
			/* If internal bus is specified, then just drop it. */
			if ((paddr = strchr(addr, '@')) != NULL) *paddr = '\0';
			port = atoi(addr);
		}
		if (myself) {
			if (node->ip == NULL && ip != NULL) {
				node->ip = ip;
				node->port = port;
			}
		}
		else {
			if (friends == NULL) continue;
			dbNode *friend = createDbNode(ip, port, cluster);
			if (friend == NULL) {
				success = 0;
				goto cleanup;
			}
			listAddNodeTail(friends, friend);
			continue;
		}
		if (name != NULL && node->name == NULL) node->name = sdsnew(name);
		node->is_replica = (strstr(flags, "slave") != NULL ||
			(master_id != NULL && master_id[0] != '-'));
		if (node->is_replica) {
			node->replicate = sdsnew(master_id);
			cluster->replicas_count++;
		}
		else cluster->masters_count++;
		/* If authentication failed on a master node, exit with success = 0 */
		/*
		if (!node->is_replica) {
			success = 0;
			goto cleanup;
		}
		*/
		if (i == 8) {
			int remaining = strlen(line);
			while (remaining > 0) {
				p = strchr(line, ' ');
				if (p == NULL) p = line + remaining;
				remaining -= (p - line);

				char *slotsdef = line;
				*p = '\0';
				if (remaining) {
					line = p + 1;
					remaining--;
				}
				else line = p;
				char *dash = NULL;
				if (slotsdef[0] == '[') {
					slotsdef++;
					if ((p = strstr(slotsdef, "->-"))) { // Migrating
						*p = '\0';
						p += 3;
						char *closing_bracket = strchr(p, ']');
						if (closing_bracket) *closing_bracket = '\0';
						sds slot = sdsnew(slotsdef);
						sds dst = sdsnew(p);
						node->migrating_count += 2;
						node->migrating =
							zrealloc(node->migrating,
							(node->migrating_count * sizeof(sds)));
						node->migrating[node->migrating_count - 2] =
							slot;
						node->migrating[node->migrating_count - 1] =
							dst;
					}
					else if ((p = strstr(slotsdef, "-<-"))) {//Importing
						*p = '\0';
						p += 3;
						char *closing_bracket = strchr(p, ']');
						if (closing_bracket) *closing_bracket = '\0';
						sds slot = sdsnew(slotsdef);
						sds src = sdsnew(p);
						node->importing_count += 2;
						node->importing = zrealloc(node->importing,
							(node->importing_count * sizeof(sds)));
						node->importing[node->importing_count - 2] =
							slot;
						node->importing[node->importing_count - 1] =
							src;
					}
				}
				else if ((dash = strchr(slotsdef, '-')) != NULL) {
					p = dash;
					int start, stop;
					*p = '\0';
					start = atoi(slotsdef);
					stop = atoi(p + 1);
					mapSlot(cluster, start, node);
					mapSlot(cluster, stop, node);
					while (start <= stop) {
						int slot = start++;
						node->slots[node->slots_count++] = slot;
					}
				}
				else if (p > slotsdef) {
					int slot = atoi(slotsdef);
					node->slots[node->slots_count++] = slot;
					mapSlot(cluster, slot, node);
				}
			}
		}
	}
cleanup:
	if (ctx != NULL) consumeRedisReaderBuffer(ctx);
	freeReplyObject(reply);
	return success;
}

int fetchDbClusterConfiguration(dbCluster *cluster,
	dbClusterEntryPoint* entry_points,
	int entry_points_count)
{
	int success = 1, i;
	redisContext *ctx = NULL;
	list *friends = NULL;
	dbClusterEntryPoint *entry_point = NULL;
	for (i = 0; i < entry_points_count; i++) {
		dbClusterEntryPoint *ep = &entry_points[i];
		if (ep->host != NULL && ep->port) {
			serverDbLog(LL_DEBUG, "Trying cluster entry point %s:%d" ,
				ep->host, ep->port);
			ctx = redisConnect(ep->host, ep->port);
		}
		else if (ep->socket != NULL) {
			serverDbLog(LL_DEBUG, "Trying cluster entry point %s", ep->socket);
			ctx = redisConnectUnix(ep->socket);
		}
		else continue;
		if (ctx->err) {
			sds err = sdsnew("Could not connect to Redis at ");
			if (ep->host != NULL) {
				err = sdscatprintf(err, "%s:%d: %s", ep->host, ep->port,
					ctx->errstr);
			}
			else {
				err = sdscatprintf(err, "%s: %s", ep->socket, ctx->errstr);
			}
			serverDbLog(LL_WARNING,"error:%s" , err);
			redisFree(ctx);
			ctx = NULL;
			sdsfree(err);
		}
		else {
			entry_point = ep;
			break;
		}
	}
	if (entry_point == NULL || ctx == NULL) {
		fprintf(stderr, "FATAL: failed to connect to Redis Cluster\n");
		return 0;
	}
	if (cluster->entry_point != NULL) {
		freeEntryPoints(cluster->entry_point, 1);
		zfree(cluster->entry_point);
	}
	cluster->entry_point = copyEntryPoint(entry_point);
	if (cluster->entry_point == NULL) {
		fprintf(stderr, "FATAL: failed to allocate cluster's entry point\n");
		return 0;
	}
	serverDbLog(LL_WARNING, "Fetching cluster configuration from entry point '%s'",
		cluster->entry_point->address);
	dbNode *firstNode =
		createDbNode(entry_point->host, entry_point->port, cluster);
	if (!firstNode) { success = 0; goto cleanup; }
	friends = listCreate();
	success = (friends != NULL);
	if (!success) goto cleanup;
	success = dbNodeLoadInfo(cluster, firstNode, friends, ctx);
	clusterAddDbNode(cluster, firstNode);
	if (!success) goto cleanup;
	listIter li;
	listNode *ln;
	listRewind(friends, &li);
	while ((ln = listNext(&li))) {
		dbNode *friend = ln->value;
		success = dbNodeLoadInfo(cluster, friend, NULL, NULL);
		if (!success) {
			listDelNode(friends, ln);
			freeDbNode(friend);
			goto cleanup;
		}
		clusterAddDbNode(cluster, friend);
	}
cleanup:
	if (friends) listRelease(friends);
	return success;
}

dbNode *searchNodeBySlot(dbCluster *cluster, int slot) {
	dbNode *node = NULL;
	raxIterator iter;
	raxStart(&iter, cluster->slots_map);
	int slot_be = htonl(slot);
	if (!raxSeek(&iter, ">=", (unsigned char*)&slot_be, sizeof(slot_be))) {
		serverDbLog(LL_WARNING, "Failed to seek cluster node into slots map.");
		raxStop(&iter);
		return NULL;
	}
	if (raxNext(&iter)) node = (dbNode *)iter.data;
	raxStop(&iter);
	return node;
}

dbNode *getNodeByName(dbCluster *cluster, const char *name) {
	if (cluster->nodes_by_name == NULL) return NULL;
	dbNode *node = NULL;
	raxIterator iter;
	raxStart(&iter, cluster->nodes_by_name);
	if (!raxSeek(&iter, "=", (unsigned char*)name, strlen(name))) {
		serverDbLog(LL_WARNING, "Failed to seek cluster node into nodes_by_name.");
		raxStop(&iter);
		return NULL;
	}
	if (raxNext(&iter)) node = (dbNode *)iter.data;
	raxStop(&iter);
	return node;
}

dbNode *getFirstMappedNode(dbCluster *cluster) {
	dbNode *node = NULL;
	raxIterator iter;
	raxStart(&iter, cluster->slots_map);
	if (!raxSeek(&iter, "^", NULL, 0)) {
		raxStop(&iter);
		return NULL;
	}
	if (raxNext(&iter)) node = (dbNode *)iter.data;
	raxStop(&iter);
	return node;
}

/* Return lexicographically sorted node names. Names are taken from the
* nodes_by_name radix tree, and the list is built in a "lazy" way, since
* it's NULL until `dbClusterGetMasterNames` is called for the very frist time.
* Furthermore, if the cluster is a duplicate, it will be taken by the
* cluster's parent. */
list *dbClusterGetMasterNames(dbCluster *cluster) {
	list *names = NULL;
	if (cluster->duplicated_from)
		names = dbClusterGetMasterNames(cluster->duplicated_from);
	if (names != NULL) return names;
	names = cluster->master_names;
	if (names == NULL) {
		if (cluster->broken || cluster->is_updating ||
			cluster->update_required || !cluster->nodes_by_name) return NULL;
		names = cluster->master_names = listCreate();
		listSetFreeMethod(names, (void(*)(void *)) sdsfree);
		if (names == NULL) {
			serverDbLog(LL_WARNING, "Failed to allocate cluster->master_names");
			return NULL;
		}
		raxIterator iter;
		raxStart(&iter, cluster->nodes_by_name);
		if (!raxSeek(&iter, "^", NULL, 0)) {
			raxStop(&iter);
			listRelease(cluster->master_names);
			return NULL;
		}
		while (raxNext(&iter)) {
			dbNode *node = iter.data;
			if (node->is_replica) continue;
			sds name = sdsnewlen(iter.key, iter.key_len);
			listAddNodeTail(names, name);
		}
		raxStop(&iter);
	}
	return names;
}

/* Update the cluster's configuration. Wait until all request pending or
* requests still writing to the cluster have finished and then fetch the
* cluster configuration again.
* Return values:
*      CLUSTER_RECONFIG_WAIT: there are requests pendng or writing
*                             to cluster, so reconfiguration will start
*                             after these queues are empty.
*      CLUSTER_RECONFIG_STARTED: reconfiguration has started
*      DB_CLUSTER_RECONFIG_ERR: some error occurred during reconfiguration.
*                            In this case cluster->broken is set to 1.
*      CLUSTER_RECONFIG_ENDED: reconfiguration ended with success. */
int updateDbCluster(dbCluster *cluster) {
	if (cluster->broken) return DB_CLUSTER_RECONFIG_ERR;
	int status = DB_CLUSTER_RECONFIG_WAIT;
	listIter li;
	listNode *ln;
	int requests_to_wait = 0, entry_points_count = 0;
	dbClusterEntryPoint *entry_points =
		zmalloc(sizeof(*entry_points) * listLength(cluster->nodes));
	if (entry_points == NULL) return DB_CLUSTER_RECONFIG_ERR;
	listRewind(cluster->nodes, &li);
	/* Count all requests_pending or request_to_send that are still
	* writing to cluster. */
	while ((ln = listNext(&li))) {
		dbNode *node = ln->value;
		sds addr = sdscatprintf(sdsempty(), "%s:%d", node->ip, node->port);
		dbClusterEntryPoint *ep = &entry_points[entry_points_count++];
		ep->host = zstrdup(node->ip);
		ep->port = node->port;
		ep->socket = NULL;
		ep->address = zstrdup(addr);
		sdsfree(addr);
		if (node->is_replica) continue;
		dbClusterConnection *conn = node->connection;
		if (conn == NULL) continue;
		requests_to_wait += listLength(conn->requests_pending);
		listIter rli;
		listNode *rln;
		listRewind(conn->requests_to_send, &rli);
		while ((rln = listNext(&rli))) {
			dbRequest *req = rln->value;
			if (req->has_write_handler) requests_to_wait++;
			else {
				/* All requests to send that aren't writing to cluster
				* are directly added to request_to_reprocess and removed
				* from the `requests_to_send` queue. */
				clusterAddRequestToReprocess(cluster, req);
				listDelNode(conn->requests_to_send, rln);
				req->requests_to_send_lnode = NULL;
			}
		}
	}
	serverDbLog(LL_DEBUG, "Cluster reconfiguration: still waiting for %d requests",
		requests_to_wait);
	cluster->is_updating = 1;
	/* If there are requests pending or writing to cluster, just return
	* CLUSTER_RECONFIG_WAIT status. */
	if (requests_to_wait) goto final;
	status = DB_CLUSTER_RECONFIG_STARTED;
	/* Start the reconfiguration. */
	serverDbLog(LL_DEBUG, "Reconfiguring cluster ");
	if (!resetDbCluster(cluster)) {
		serverDbLog(LL_WARNING, "Failed to reset cluster!");
		status = DB_CLUSTER_RECONFIG_ERR;
		goto final;
	}
	if (!fetchDbClusterConfiguration(cluster, entry_points, entry_points_count)) {
		serverDbLog(LL_WARNING, "Failed to fetch cluster configuration!");
		status = DB_CLUSTER_RECONFIG_ERR;
		goto final;
	}
	/* Re-process all the requests that were moved to
	* `cluster->requests_to_reprocess` */
	raxIterator iter;
	raxStart(&iter, cluster->requests_to_reprocess);
	if (!raxSeek(&iter, "^", NULL, 0)) {
		raxStop(&iter);
		status = DB_CLUSTER_RECONFIG_ERR;
		serverDbLog(LL_WARNING, "Failed to reset 'cluster->requests_to_reprocess'");
		goto final;
	}
	cluster->is_updating = 0;
	cluster->update_required = 0;
	serverDbLog(LL_DEBUG, "Reprocessing cluster requests ");
	while (raxNext(&iter)) {
		dbRequest *req = (dbRequest *)iter.data;
		req->need_reprocessing = 0;
		if (raxRemove(cluster->requests_to_reprocess, iter.key, iter.key_len,
			NULL)) raxSeek(&iter, ">", iter.key, iter.key_len);
		/*processRequest(req, NULL, NULL);*/
	}
	raxStop(&iter);
	serverDbLog(LL_DEBUG, "Cluster reconfiguration ended");
	status = DB_CLUSTER_RECONFIG_ENDED;
	final:
	if (entry_points) {
		freeEntryPoints(entry_points, entry_points_count);
		zfree(entry_points);
	}
	if (status == DB_CLUSTER_RECONFIG_ERR) cluster->broken = 1;
	return status;
}

/* Add the request to `cluster->requests_to_reprocess` rax. Also add it
* to the client's `requests_to_reprocess` list.
* The request's node will also be set to NULL (since the current configuration
* will be reset) and `need_reprocessing` will be set to 1.
* The `written` count will be also set to 0, since the request must be
* written to the cluster again when the new cluster's configuration will be
* available. */
void clusterAddRequestToReprocess(dbCluster *cluster, void *r) {
	dbRequest *req = r;
	req->need_reprocessing = 1;
	req->node = NULL;
	/*req->slot = -1;*/
	req->written = 0;
	char *fmt = "%" PRId64;
	sds id = sdscatprintf(sdsempty(), fmt, req->id);
	raxInsert(cluster->requests_to_reprocess, (unsigned char *)id,
		sdslen(id), req, NULL);
	sdsfree(id);
}

void clusterRemoveRequestToReprocess(dbCluster *cluster, void *r) {
	dbRequest *req = r;
	req->need_reprocessing = 0;
	char *fmt = "%" PRId64;
	sds id = sdscatprintf(sdsempty(), fmt, req->id);
	raxRemove(cluster->requests_to_reprocess, (unsigned char *)id,
		sdslen(id), NULL);
	sdsfree(id);
}

dbClusterEntryPoint *copyEntryPoint(dbClusterEntryPoint *source) {
	dbClusterEntryPoint *ep = zmalloc(sizeof(*ep));
	if (ep == NULL) return NULL;
	ep->host = (source->host ? zstrdup(source->host) : NULL);
	ep->socket = (source->socket ? zstrdup(source->socket) : NULL);
	ep->address = (source->address ? zstrdup(source->address) : NULL);
	ep->port = source->port;
	if (ep->address == NULL) {
		if (ep->host && ep->port) {
			sds addr = sdscatprintf(sdsempty(), "%s:%d", ep->host, ep->port);
			ep->address = zstrdup(addr);
			sdsfree(addr);
		}
		else if (ep->socket) ep->address = zstrdup(ep->socket);
	}
	return ep;
}

void freeEntryPoints(dbClusterEntryPoint *entry_points, int count) {
	int i;
	for (i = 0; i < count; i++) {
		dbClusterEntryPoint *entry_point = &(entry_points[i]);
		if (entry_point->address) zfree(entry_point->address);
		if (entry_point->host) zfree(entry_point->host);
		if (entry_point->socket) zfree(entry_point->socket);
	}
}

dbNode *getNodeByKey(dbCluster *cluster, char *key, int keylen)
{
	dbNode *node = NULL;
	int slot = clusterKeyHashSlot(key, keylen);
	node = searchNodeBySlot(cluster, slot);
	return node;
}

static dbClusterConnection *getRequestConnection(dbRequest *req) {
	dbNode *node = req->node;
	if (node == NULL) return NULL;
	return node->connection;
}

static dbRequest *getFirstQueuedRequest(list *queue, int *is_empty) {
	if (is_empty != NULL) *is_empty = 0;
	listNode *ln = listFirst(queue);
	if (ln == NULL) {
		if (is_empty != NULL) *is_empty = 1;
		return NULL;
	}
	return (dbRequest *)ln->value;
}

static int enqueueRequest(dbRequest *req, int queue_type) {
	dbClusterConnection *conn = getRequestConnection(req);
	if (conn == NULL) return 0;
	int success = 0;
	int *sp = &success;
	if (queue_type == QUEUE_TYPE_SENDING) {
		addObjectToList(req, conn, requests_to_send, sp);
	}
	else if (queue_type == QUEUE_TYPE_PENDING) {
		addObjectToList(req, conn, requests_pending, sp);
	}
	return success;
}

static void dequeueRequest(dbRequest *req, int queue_type) {
	dbClusterConnection *conn = getRequestConnection(req);
	if (conn == NULL) return; 
	if (queue_type == QUEUE_TYPE_SENDING) {
		removeObjectFromList(req, conn, requests_to_send);
	}
	else if (queue_type == QUEUE_TYPE_PENDING) {
		removeObjectFromList(req, conn, requests_pending);
	}
}

void freeDbRequest(dbRequest *req) {
	if (req->buffer)
		sdsfree(req->buffer); 
	if(req->keyobj)
		decrRefCount(req->keyobj);
	if (req->value_obj)
		decrRefCount(req->value_obj);
	for (int i = 0 ; i < 5;i++) {
		if (req->argv[i] != NULL)
			decrRefCount(req->argv[i]); 
	}
}

static void freeDbRequestList(list *request_list) {
	if (request_list == NULL) return;
	listIter li;
	listNode *ln;
	listRewind(request_list, &li);
	while ((ln = listNext(&li))) {
		dbRequest *req = ln->value;
		listDelNode(request_list, ln);
		if (req == NULL) continue;
		if (ln == req->requests_to_send_lnode)
			req->requests_to_send_lnode = NULL;
		else if (ln == req->requests_pending_lnode)
			req->requests_pending_lnode = NULL;
		freeDbRequest(req);
	}
	listRelease(request_list);
}

/* Try to call aeCreateFileEvent, and if ERANGE error has been issued, try to
* resize event loop's setsize to fd + proxy.min_reserved_fds.
* The 'retried' argument is used to ensure that resizing will be tried only
* once. */
static int installIOHandler(aeEventLoop *el, int fd, int mask, aeFileProc *proc, void *data)
{
	if (aeCreateFileEvent(el, fd, mask, proc, data) != AE_ERR) {
		return 1;
	}
	else {
		serverDbLog(LL_WARNING , "Could not create read handler: %s", strerror(errno));
		return 0;
	}
}

/* This function does the same things as redisReaderGetReply, but
* it does not trim the reader's buffer, in order to let the proxy's
* read handler to get the full reply's buffer. Consuming and trimming
* ther reader's buffer is up to the proxy. */
static int __hiredisReadReplyFromBuffer(redisReader *r, void **reply) {
	/* Default target pointer to NULL. */
	if (reply != NULL)
		*reply = NULL;

	/* Return early when this reader is in an erroneous state. */
	if (r->err)
		return REDIS_ERR;

	/* When the buffer is empty, there will never be a reply. */
	if (r->len == 0)
		return REDIS_OK;

	/* Set first item to process when the stack is empty. */
	if (r->ridx == -1) {
		r->rstack[0].type = -1;
		r->rstack[0].elements = -1;
		r->rstack[0].idx = -1;
		r->rstack[0].obj = NULL;
		r->rstack[0].parent = NULL;
		r->rstack[0].privdata = r->privdata;
		r->ridx = 0;
	}

	/* Process items in reply. */
	while (r->ridx >= 0)
		if (processItem(r) != REDIS_OK)
			break;

	/* Return ASAP when an error occurred. */
	if (r->err)
		return REDIS_ERR;

	/* Emit a reply when there is one. */
	if (r->ridx == -1) {
		if (reply != NULL)
			*reply = r->reply;
		r->reply = NULL;
	}
	return REDIS_OK;
}

static int processClusterReplyBuffer(redisContext *ctx, dbNode *node)
{
	if (node == NULL) return 0;
	char *errmsg = NULL;
	void *_reply = NULL;
	redisReply *reply = NULL;
	int replies = 0;
	while (ctx->reader->len > 0) {
		int ok = (__hiredisReadReplyFromBuffer(ctx->reader, &_reply) == REDIS_OK);
		int do_break = 0, is_cluster_err = 0;
		dbCluster *cluster = NULL;
		if (!ok) {
			serverDbLog(LL_WARNING , "Error reading from node %s:%d : %s",
				node->ip, node->port,  ctx->errstr);
			errmsg = ERROR_DB_CLUSTER_READ_FAIL;
		}
		reply = (redisReply *)_reply;
		/* Reply not yet available, just return */
		if (ok && reply == NULL) break;
		replies++;
		dbRequest *req = getFirstRequestPending(node, NULL);
		int free_req = 1;
		/* If request is NULL, it's a ghost request that is a NULL
		* placeholder in place of a request created by a freed client
		* (ie. a disconnected client). In this case, just dequeue the list
		* node containing the NULL placeholder and directly skip to
		* 'consume_buffer' in order to process the remaining reply buffer. */
		if (req == NULL) {
			list *queue = node->connection->requests_pending;
			/* It should never happen that the request is NULL because of an
			* empty queue while we still have reply buffer to process */
			if (listLength(queue) == 0) {
				serverDbLog(LL_WARNING , "listLength(queue) > 0");
				assert(listLength(queue) > 0);
			}
			listNode *ln = listFirst(queue);
			listDelNode(queue, ln);
			goto consume_buffer;
		}

		serverDbLog(LL_DEBUG, "Reply read complete for db request " REQID_PRINTF_FMT
			", %s%s", REQID_PRINTF_ARG(req),
			errmsg ? " ERR: " : "OK!",
			errmsg ? errmsg : "");

		dequeuePendingRequest(req);
		cluster = server.db_cluster;
		assert(cluster != NULL);
		if (reply->type == REDIS_REPLY_ERROR) {
			assert(reply->str != NULL);
			/* In case of ASK|MOVED reply the cluster need to be
			* reconfigured.
			* In this case we suddenly set `cluster->is_updating` and
			* we add the request to the clusters' `requests_to_reprocess`
			* pool (the request will be also added to a
			* `requests_to_reprocess` list on the client). */
			if ((strstr(reply->str, "ASK") == reply->str ||
				strstr(reply->str, "MOVED") == reply->str))
			{
				serverDbLog(LL_DEBUG , "db cluster configuration changed! ");
				cluster->update_required = 1;
				/* Automatic cluster update is posticipated when the client
				* is under a MULTI transaction. */
				cluster->is_updating = 1;
				is_cluster_err = 1;
				clusterAddRequestToReprocess(cluster, req);
				/* We directly jump to `consume_buffer` since we won't
				* reply to the client now, but after the reconfiguration
				* ends. */
				goto consume_buffer;
			}
		}
		if(errmsg != NULL)
			goto consume_buffer;
		char *obuf = ctx->reader->buf;
		UNUSED(obuf); 
		/*size_t len = ctx->reader->len;*/
		size_t len = ctx->reader->pos;
		if (len > ctx->reader->len) len = ctx->reader->len;
		// success
		/* removeEvictKey(req->keyobj, req->dbid, 0); */
		if (req->request_type == REQUEST_READ) {
			sdsfree(req->buffer);
			req->buffer = NULL; 
			if (reply->type == REDIS_REPLY_STRING) {
				rio payload;
				sds rep_str = sdsnewlen(reply->str, reply->len); 
				if (verifyDumpPayload(reply->str, reply->len) == C_ERR)
					goto consume_buffer; 
				rioInitWithBuffer(&payload, rep_str);
				int type = rdbLoadObjectType(&payload);	
				if (type == -1) {
					sdsfree(rep_str); 
					goto consume_buffer;
				}
				req->value_obj = rdbLoadObject(type, &payload, reply->str);
				if (req->value_obj == NULL) {
					sdsfree(rep_str);
					goto consume_buffer;
				}
				/* restore -> aof & slave */
				/*sdsfree(req->buffer);*/ 
				req->argv[0] = createStringObject("restore", 7); 
				req->argv[1] = createStringObject(req->keyobj->ptr, sdslen(req->keyobj->ptr));
				req->argv[2] = createStringObject("0", 1);
				req->argv[3] = createStringObject(reply->str, reply->len);
				req->argv[4] = createStringObject("replace", 7);
 				//req->buffer = sdscatprintf(sdsempty(), "*4\r\n$7\r\nrestore\r\n$%i\r\n%s\r\n$1\r\n0\r\n$%i\r\n", sdslen(req->keyobj->ptr), req->keyobj->ptr, reply->len);
				//req->buffer = sdscatlen(req->buffer, reply->str, reply->len);
				//req->buffer = sdscatlen(req->buffer, "\r\n", 2);

				sdsfree(rep_str);
			}
		}
		asyncPostDbResponse(req); 
		req = NULL; 
	consume_buffer:
		/* If cluster has been set is reconfiguring state, we call the
		* startClusterReconfiguration function. */
		if (cluster && cluster->is_updating) {
			int reconfig_status = updateDbCluster(cluster);
			do_break = (reconfig_status == DB_CLUSTER_RECONFIG_ENDED);
			if (!do_break) {
				/* If reconfiguration failed, reply the error to the client,
				* elsewhere we're still waiting for all requests pending
				* to finish before reconfigration actually starts.
				* In the latter case, we don't do anything. */
				if (reconfig_status == DB_CLUSTER_RECONFIG_ERR) {
					serverDbLog(LL_WARNING ,"db cluster reconfiguration failed!");
					do_break = 1;
				}
			}
			else {
				/* Reconfiguration ended with success. If reply was ASK or
				* MOVED, the request has been enqueued again and, since
				* need_reprocessing is now 0, we set it to NULL in order to
				* avoid freeing it.
				* For all other requests, their reply has been added so
				* they can be freed, but since their node was related to
				* the old configuration and it has now been freed,
				* we just set their node to NULL. */
				if (is_cluster_err) req = NULL;
				else if (req) req->node = NULL;
			}
			if (do_break) goto clean;
		}
		/* Consume reader buffer */
		consumeRedisReaderBuffer(ctx);
	clean:
		freeReplyObject(reply);
		if (req && free_req) freeDbRequest(req);
		if (!ok || do_break) break;
	}
	return replies;
}

static int writeToCluster(aeEventLoop *el, int fd, dbRequest *req) {
	size_t buflen = sdslen(req->buffer);
	int nwritten = 0;
	while (req->written < buflen) {
		nwritten = write(fd, req->buffer + req->written, buflen - req->written);
		if (nwritten <= 0) break;
		req->written += nwritten;
	}
	if (nwritten == -1) {
		if (errno == EAGAIN) 
			nwritten = 0;
		else {
			serverDbLog(LL_WARNING , "Error writing request to cluster: %s" , strerror(errno));
			if (errno == EPIPE) 
				dbNodeDisconnect(req->node);
			else 
				freeDbRequest(req);
			return 0;
		}
	}
	int success = 1;
	if (req->written == buflen) {
		/* The whole query has been written, so install the read handler and
		* move the request from requests_to_send to requests_pending. */

		serverDbLog(LL_DEBUG , "Request " REQID_PRINTF_FMT " written to node %s:%d, "
			"adding it to pending requests",
			REQID_PRINTF_ARG(req),
			req->node->ip, req->node->port);

		aeDeleteFileEvent(el, fd, AE_WRITABLE);
		if (req->has_write_handler) 
			req->has_write_handler = 0;
		dequeueRequestToSend(req);
		if (!enqueuePendingRequest(req)) {
			serverDbLog(LL_WARNING , "Could not enqueue pending request "
				REQID_PRINTF_FMT,
				REQID_PRINTF_ARG(req));

			freeDbRequest(req);
			return 0;
		}
	}
	else {
		/* Request has not been completely written, so try to install the write
		* handler. */
		if (!installIOHandler(el, fd, AE_WRITABLE, writeToClusterHandler,
			req->node))
		{
			serverDbLog(LL_WARNING , "Failed to create write handler for request "
				REQID_PRINTF_FMT, REQID_PRINTF_ARG(req));
			freeDbRequest(req);
			return 0;
		}
		req->has_write_handler = 1;

		serverDbLog(LL_DEBUG , "Write handler installed into request " REQID_PRINTF_FMT
			" for node %s:%d", REQID_PRINTF_ARG(req),
			req->node->ip, req->node->port);

	}
	return success;
}

static void readClusterReply(aeEventLoop *el, int fd,
	void *privdata, int mask)
{
	UNUSED(mask);
	UNUSED(fd);
	UNUSED(el);
	dbClusterConnection *connection = privdata;
	if (connection == NULL) return;
	redisContext *ctx = connection->context;
	dbNode *node = connection->node;
	dbRequest *req = NULL;
	list *queue = connection->requests_pending;
	if (node != NULL)
		req = getFirstRequestPending(node, NULL);
	sds errmsg = NULL;
	char *ip = ctx->tcp.host;
	int port = ctx->tcp.port;
	serverDbLog(LL_DEBUG , "Reading reply from %s:%d ...",
		ip, port);
	int success = (redisBufferRead(ctx) == REDIS_OK), replies = 0,
		node_disconnected = 0;
	if (!success) {
		serverDbLog(LL_DEBUG, "Failed redisBufferRead from %s:%d",
			ip, port);
		int err = ctx->err;
		node_disconnected = (err & (REDIS_ERR_IO | REDIS_ERR_EOF));
		if (node_disconnected) errmsg = sdsnew(ERROR_DB_NODE_DISCONNECTED);
		else {
			serverDbLog(LL_WARNING , "Error from db node %s:%d: %s", ip, port,
				ctx->errstr);
			errmsg = sdsnew("Failed to read reply from dbnode ");
		}
		errmsg = sdscatfmt(errmsg, "%s:%u", ip, port);
		/* An error occurred, so dequeue the request. If the request is not
		* NULL, send an error reply to the client and then free the requests
		* itself. If the node is down (node_disconnected), call
		* clusterNodeDisconnect in order to close the socket, close the file
		* event handlers and close all the other requests waiting for a
		* reply on the same node's socket. */
		if (req != NULL) {
			dequeuePendingRequest(req);
			freeDbRequest(req);
		}
		else {
			listNode *first = listFirst(queue);
			if (first) listDelNode(queue, first);
		}
		if (node_disconnected) {
			serverDbLog(LL_WARNING, "%s", errmsg);
			if (node) dbNodeDisconnect(node);
		}
		sdsfree(errmsg);
		/* Exit, since an error occurred. */
		return;
	}
	else replies = processClusterReplyBuffer(ctx, node);
	UNUSED(replies);
	if (errmsg != NULL) sdsfree(errmsg);
}

static dbRequest *handleNextRequestsToCluster(dbNode *node,
	dbRequest **failed)
{
	dbRequest *req = NULL;
	while ((req = getFirstRequestToSend(node, NULL))) {
		int sent = sendRequestToDbCluster(req, NULL);
		if (!sent) {
			/* Sending request failed and request has already been freed. */
			if (failed != NULL && *failed == NULL) *failed = req;
			continue;
		}
		/* If request has not been completely written (it has a write handler
		* installed, just break for now. */
		if (req->has_write_handler) break;
	}
	return req;
}


static void writeToClusterHandler(aeEventLoop *el, int fd, void *privdata,
	int mask)
{
	UNUSED(mask);
	dbClusterConnection *connection = privdata;
	if (connection == NULL) return;
	dbNode *node = connection->node;
	redisContext *ctx = connection->context;
	if (ctx == NULL) return;
	dbRequest *req = NULL;
	char *ip = ctx->tcp.host;
	int port = ctx->tcp.port;
	if (node != NULL) req = getFirstRequestToSend(node, NULL);
	if (ctx->err) {
		serverDbLog(LL_NOTICE ,"Failed to connect to node %s:%d", ip, port);
		if (req != NULL) 
			freeDbRequest(req);
		return;
	}
	if (!connection->has_read_handler) {
		if (!installIOHandler(el, ctx->fd, AE_READABLE, readClusterReply,connection))
		{
			serverDbLog(LL_NOTICE , "Failed to create read reply handler for db node %s:%d",
				ip, port);
			if (req != NULL) 
				freeDbRequest(req);
			return;
		}
		else {
			connection->has_read_handler = 1;
			if (node != NULL) {
				serverDbLog(LL_DEBUG ,"Read reply handler installed "
					"for db node %s:%d", ip, port);
			}
		}
	}
	connection->connected = 1;
	/* Delete the file handlers from the event loop if it's a connection in
	* the thread's connections pool (node == NULL). */
	if (node == NULL) {
		aeDeleteFileEvent(el, ctx->fd, AE_WRITABLE | AE_READABLE);
		connection->has_read_handler = 0;
		return;
	}
	if (req == NULL) return;

	int ok = writeToCluster(el, fd, req);
	/* Try to send next requests enqueued to the same cluster node if
	* the request failed or if the request have been completely written. */
	if (ok && req->has_write_handler) return;
	handleNextRequestsToCluster(node, NULL);

}

static int sendRequestToDbCluster(dbRequest *req, sds *errmsg)
{
	if (errmsg != NULL) *errmsg = NULL;
	if (req->has_write_handler) return 1;
	assert(req->node != NULL);
	dbCluster *cluster = server.db_cluster;
	assert(cluster != NULL);
	redisContext *ctx = getClusterNodeContext(req->node);
	if (ctx == NULL) {
		if ((ctx = dbNodeConnect(req->node)) == NULL) {
			sds err = sdsnew("Could not connect to node ");
			err = sdscatfmt(err, "%s:%u", req->node->ip, req->node->port);
			serverDbLog(LL_DEBUG , "%s", err);
			if (errmsg != NULL) {
				/* Remember to free the string outside this function*/
				*errmsg = err;
			}
			else sdsfree(err);
			freeDbRequest(req);
			return 0;
		}
		/* Install the write handler since the connection to the cluster node
		* is asynchronous. */
		if (!installIOHandler(server.db_el, ctx->fd, AE_WRITABLE, writeToClusterHandler,
			req->node->connection))
		{
			serverDbLog(LL_WARNING , "Failed to create write handler for request "
				REQID_PRINTF_FMT, REQID_PRINTF_ARG(req));
			freeDbRequest(req);
			return 0;
		}
		req->has_write_handler = 1;
		serverDbLog(LL_DEBUG , "Write handler installed into request " REQID_PRINTF_FMT
			" for node %s:%d", REQID_PRINTF_ARG(req),
			req->node->ip, req->node->port);
		return 1;
	}
	else if (!isClusterNodeConnected(req->node)) {
		return 1;
	}
	dbClusterConnection *conn = req->node->connection;
	assert(conn != NULL);
	if (!conn->has_read_handler) {
		if (!installIOHandler(server.db_el, ctx->fd, AE_READABLE, readClusterReply,
			conn))
		{
			serverDbLog(LL_WARNING , "Failed to create read reply handler for node %s:%d",
				req->node->ip, req->node->port);
			freeDbRequest(req);
			return 0;
		}
		else {
			conn->has_read_handler = 1;
			serverDbLog(LL_DEBUG , "Read reply handler installed "
				"for node %s:%d", req->node->ip, req->node->port);
		}
	}
	if (!writeToCluster(server.db_el, ctx->fd, req)) return 0;
	return 1;
}

dbRequest *createDbRequest(int request_type)
{
	dbRequest *req = zcalloc(sizeof(*req));
	if (req == NULL) 
		return NULL;
	req->id = server.next_request_id++;
	req->keyobj = NULL;
	req->dbid = 0; 
	req->buffer = sdsempty();
	if (req->buffer == NULL)
		return NULL; 
	for (int i= 0 ; i < 5 ; i++) {
		req->argv[i] = NULL; 
	}
	req->node = NULL; 
	req->written = 0;
	req->has_write_handler = 0;
	req->need_reprocessing = 0;
	req->request_type = request_type; 
	req->value_obj = NULL; 
	req->client_id = 0; 
	req->param_ex = 0; 
	req->requests_pending_lnode = NULL;
	req->requests_to_send_lnode = NULL;
	server.db_cluster->request_count++; 
	return req; 
}

int postDbRequest(dbRequest *req)
{
	serverDbLog(LL_DEBUG , "post db request ." REQID_PRINTF_FMT ,
		REQID_PRINTF_ARG(req));
	/*uint64_t req_id = req->id;*/
	dbCluster *cluster = server.db_cluster;
	assert(cluster != NULL);
	sds errmsg = NULL;
	if (cluster->broken) {
		errmsg = sdsnew(ERROR_DB_CLUSTER_RECONFIG);
		goto invalid_request;
	}
	else if (cluster->is_updating) {
		clusterAddRequestToReprocess(cluster, req);
		return 1;
	}
	dbNode *node = getNodeByKey(cluster , req->keyobj->ptr, sdslen(req->keyobj->ptr));
	if (node == NULL) {
		if (errmsg == NULL)
			errmsg = sdsnew(ERROR_NO_DB_NODE);
		goto invalid_request;
	}
	req->node = node; 
	if (!enqueueRequestToSend(req)) goto invalid_request;
	dbRequest *failed_req = NULL;
	handleNextRequestsToCluster(req->node, &failed_req);
	if (failed_req == req) 
		goto invalid_request;
	return 1;
invalid_request:
	serverDbLog(LL_WARNING, "post db request invalid." REQID_PRINTF_FMT,
		REQID_PRINTF_ARG(req));
	if (errmsg != NULL) {
		sdsfree(errmsg);
		freeDbRequest(req);
		return 1;
	}
	freeDbRequest(req);
	return 0;
}


void initDbCluster()
{
	server.db_cluster = createDbCluster();
	if (server.db_cluster == NULL) {
		serverPanic("ERROR .Failed to create db cluster .");
		exit(1);
	}
	if (!fetchDbClusterConfiguration(server.db_cluster, server.db_cluster_config.db_entry_points,
		server.db_cluster_config.db_entry_points_count))
	{
		serverPanic("ERROR: Failed to fetch db cluster configuration!");
		exit(1);
	}
}
