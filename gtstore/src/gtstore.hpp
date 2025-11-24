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

#define DEF_PART_COUNT 7
#define DEF_REP 3

#define MAX_KEY_BYTE_PER_REQUEST 20
#define MAX_VALUE_BYTE_PER_REQUEST 1000

using namespace std;
using grpc::Status;
using grpc::ServerContext;
using grpc::ServerBuilder;
using grpc::Server;

typedef vector<string> val_t;

// this is to ensure that we can send data to the network in an understood manner (gRPC msg format)
inline void convert_to_protobuf(const val_t &src, gtstore::Value *dest) {
	for (int i = 0; i < src.size(); i++) {
		string str = src[i];
		dest->add_items(str);
	}
}

// now when we recevie data from the network, we want to convert it back itno a vector that we can use
inline val_t convert_from_protobuf(const gtstore::Value &src) {
	val_t value;
	for (int i = 0; i < src.items_size(); i++) {
		string str = src.items(i);
		value.push_back(str);
	}
	return value;
}

// this function will calculate which partition (bucket) to place a key ==> SHARDING !!!!!!!
inline int get_bucket_id(string key, int num_parts) {
	std::hash<std::string> hash; // this is used for the hashing
	// then we want to hash the key and then place it in its respective "bucket" based on the modulo
	size_t hash_val = hash(key);
	int result = hash_val%num_parts;
	return result;
}

// client info
class GTStoreClient {
		private:
				int client_id;
				// this is how we can communicate with the manager
				unique_ptr<gtstore::ManagerService::Stub> manager_stub;
				// this is a way for us to interact with the storage nodes
				// in this case the key is the IP addr of the node, and value = the stub
				map<string, unique_ptr<gtstore::StorageService::Stub>> node_stubs;
				// this is a function that is used to find the correct connection to interact with
				gtstore::StorageService::Stub *get_node_stub(string address);
				//val_t value;
		public:
				void init(int id);
				void finalize();
				val_t get(string key);
				bool put(string key, val_t value);
};

// this is info for a storage node
struct NodeMeta {
	int id;
	string addr;
	bool is_alive;
	// this is used by the manager to connect to the node (used for "pings")
		// what else???
	shared_ptr<gtstore::StorageService::Stub> stub;
};

class GTStoreManager {
		private:
				// handles incoming requests for the manager
				class ManagerService final:public gtstore::ManagerService::Service {
						GTStoreManager *parent; // used to access vars in main class
						public:
								ManagerService(GTStoreManager *par) : parent(par) {}
								Status Register(ServerContext *context, const gtstore::RegisterNodeRequest *req, gtstore::RegisterNodeResponse *resp) override;
								Status GetNodeForKey(ServerContext *context, const gtstore::GetNodeForKeyRequest* req, gtstore::GetNodeForKeyResponse* resp) override;
				};
				// list of all nodes that exist on our system
				std::thread monitor_thread; // this is used to send "ping" requests
				//these will be given my CML --> --partitions --replications or something flags

				void check_nodes(); // this is to consistently ping nodes for alive/dead status checks
				void handle_node_failure(int node_id); // if a node has crashed or died, this is the handler for it
					// ==> WE must copy over its contents from some other node to another node that is free
		public:
				std::map<int, NodeMeta> nodes;
				std::mutex node_mutex;
				int next_node_id = 0; // id for the next created node
				int num_parts; // number of partitions
				int rep_factor; // number of replicas
				// initializes the default values
				GTStoreManager() : num_parts(DEF_PART_COUNT), rep_factor(DEF_REP) {}
				void init(int n, int k); // accepts n nodes and k replicas as cml args
};

// Partition
// here we will put data AND individual locks for each partition so that we cna write to the same node
	// at once, but not the same partition, before would have costed extreme performance overhead
struct Bucket {
	std::unordered_map<string, val_t> data_map; // this stores the actual data (key,val pairs)
	std::mutex bucket_mutex; // mutex for each of the buckets so that we can still write to the current NODE
};

// storage info
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
				int num_parts = 0;
				vector<unique_ptr<Bucket>> buckets; // this is used to enable recovering a dead node's data and replicating it a lot easier
				//std::mutex store_mutex; took this out bc now we have the mutexes for each bucket in the node
				unique_ptr<gtstore::ManagerService::Stub> manager_stub; // this is what allows us to connect to the "Manager"
		public:
				void init(int port);
};

#endif
