#ifndef GTSTORE
#define GTSTORE

#include <string>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>
// ADDED the below includes
#include <thread>
#include <mutex>
#include <map>
#include <unordered_map>
#include <memory>
#include <functional>
#include <grpcpp/grpcpp.h>
#include "gtstore.grpc.pb.h" 

#define DEF_BUCKET_COUNT 7
#define DEF_REP 1

#define MAX_KEY_BYTE_PER_REQUEST 20
#define MAX_VALUE_BYTE_PER_REQUEST 1000

using namespace std;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerBuilder;
using grpc::Server;

typedef vector<string> val_t;

// To send data to another system (it can read it too)
inline void convert_to_protobuf(const val_t &src, gtstore::Value *dest) {
	for (int i = 0; i < src.size(); i++) {
		string str = src[i];
		dest->add_items(str);
	}
}

// To retrieve data from another system and read it properly and work w/ it in way we expect
inline val_t convert_from_protobuf(const gtstore::Value &src) {
	val_t value;
	for (int i = 0; i < src.items_size(); i++) {
		string str = src.items(i);
		value.push_back(str);
	}
	return value;
}

/**
 * Canonical shard index
 * Bucket ID == Primary Node ID given (node ids are 0...N-1 and num_buckets == N)
 * Strict Hashing: Key will always hash to the same Node ID, hence it essentially is giving us the Primary Node ID
 * B/c of bucket<==>node one-to-one correspondence, they are equal
 */
inline int get_bucket_id(string key, int num_buckets) {
	std::hash<std::string> hash;
	size_t hash_val = hash(key);
	// This is "Strict Hashing" --> map a key directly to a specific bucket
	int result = hash_val % num_buckets;
	return result;
}

// client info
class GTStoreClient {
		private:
				int client_id;
				unique_ptr<gtstore::ManagerService::Stub> manager_stub;
				// <Node_IPAddr, stub>
				map<string, unique_ptr<gtstore::StorageService::Stub>> node_stubs;
				// Retrieves the correct stub for the node client is directed to
				gtstore::StorageService::Stub *get_node_stub(string address);
		public:
				void init(int id);
				void finalize();
				val_t get(string key);
				bool put(string key, val_t value);
};

// Stores metadata for a Storage Node
struct NodeMeta {
	int id;
	string addr;
	bool is_alive;
	shared_ptr<gtstore::StorageService::Stub> stub;
};

class GTStoreManager {
		private:
				class ManagerService final:public gtstore::ManagerService::Service {
						GTStoreManager *parent; // used to access vars in main class
						public:
								ManagerService(GTStoreManager *par) : parent(par) {}
								Status Register(ServerContext *context, const gtstore::RegisterNodeRequest *req, gtstore::RegisterNodeResponse *resp) override;
								Status GetNodeForKey(ServerContext *context, const gtstore::GetNodeForKeyRequest* req, gtstore::GetNodeForKeyResponse* resp) override;
				};
				std::thread monitor_thread; // background heartbeat thread --> send ping requests to ALL nodes in time intervals

				void check_nodes(); // continuously executed by background thread (indefinitely)
				void handle_node_failure(int node_id);
		public:
				std::map<int, NodeMeta> nodes;
				// Used when modifying + reading "nodes" metadata
				std::mutex node_mutex;
				int next_node_id = 0;
				int num_buckets; // number of partitions
				int rep_factor; // number of replicas (replication)
				// initializes the default values
				GTStoreManager() : num_buckets(DEF_BUCKET_COUNT), rep_factor(DEF_REP) {}
				void init(int n, int k);
};

/**
 * Stores data and has individual locks for each partition (bucket)
 * Fine-grained locking = Bucket-level locking
 * Allows for writing to multiple buckets on a Node concurrently
 * Better iteration than previous: Node level locking
 */

struct Bucket {
	std::unordered_map<string, val_t> data_map; // this stores the actual data (key,val pairs)
	std::mutex bucket_mutex; // mutex for each of the buckets so that we can still write to the current NODE
};

class GTStoreStorage {
		private:
				class StorageService final:public gtstore::StorageService::Service {
						GTStoreStorage *parent;
						public:
								StorageService(GTStoreStorage *par) : parent(par) {}
								Status Put(ServerContext *context, const gtstore::PutRequest *req, gtstore::PutResponse *resp) override;
								Status Get(ServerContext *context, const gtstore::GetRequest *req, gtstore::GetResponse *resp) override;
								Status Ping(ServerContext *context, const gtstore::PingRequest *req, gtstore::PingResponse *resp) override;
								Status TransferData(ServerContext *context, const gtstore::TransferDataRequest *req, gtstore::TransferDataResponse *resp) override;
				};
				int node_id;
				string my_addr;
				string man_addr;
				// we get this after we have registered the manager and the manager gives us this information
				int num_buckets = 0;
				/* based on design decision: # bukets = # of nodes ManagerService was created with
				   and there is a one to one mapping i.e.
				   	- bucket[0] for ALL nodes corresponds to Node 0's data
					- bucket[1] for ALL nodes corresponds to Node 1's data
					...
					what this means:
						- on Node 2, replica data for a node, will be located in bucket # = node #
							- for Node 2, if NOT bucket[2] then we are looking at possible replica data if bucket is NON empty */
				vector<unique_ptr<Bucket>> buckets; // this is used to enable recovering a dead node's data and replicating it a lot easier
				unique_ptr<gtstore::ManagerService::Stub> manager_stub; // this is what allows us to connect to the "Manager"
		public:
				void init(int port);
};

#endif