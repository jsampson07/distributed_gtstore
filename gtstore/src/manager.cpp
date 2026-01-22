#include "gtstore.hpp"
#include <cstring>

/**
 * This is how the Manager "registers" a Node
 * Manager sends a response back to the Node with information such as node_id, if creation was successful, and buket count for the node (same across all nodes)
 * @return Status
 */
Status GTStoreManager::ManagerService::Register(ServerContext *context, const gtstore::RegisterNodeRequest *req, gtstore::RegisterNodeResponse *resp) {
	parent->node_mutex.lock(); // to ensure multiple clients can only create nodes one at a time
	int id = parent->next_node_id;
	parent->next_node_id++;
	NodeMeta new_node;
	new_node.id = id;
	new_node.addr = req->address();
	new_node.is_alive = true;
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(new_node.addr, grpc::InsecureChannelCredentials());
	new_node.stub = gtstore::StorageService::NewStub(channel);
	parent->nodes[id] = new_node;
	resp->set_node_id(id);
	resp->set_success(new_node.is_alive);
	resp->set_bucket_count(parent->num_buckets);
	parent->node_mutex.unlock();
	cout << "Manager has responded to Register request, created new node ID: " << new_node.id << " Address: " << new_node.addr << "\n";
	return Status::OK;
}

/**
 * This function is to get the Node corresponding to a key (whether a user wants to create a new key-val pair or access data)
 * @return Status
 */
Status GTStoreManager::ManagerService::GetNodeForKey(ServerContext *context, const gtstore::GetNodeForKeyRequest *req, gtstore::GetNodeForKeyResponse *resp) {
	parent->node_mutex.lock(); // we want to lock to make sure no other nodes get accessed while we are trying to select nodes to use to store or retrieve
	vector<int> node_arr; // get all nodes on the system (but just their node IDs)
	std::map<int, NodeMeta>::iterator iterator;
	for (iterator = parent->nodes.begin(); iterator != parent->nodes.end(); iterator++) {
		node_arr.push_back(iterator->first);
	}
	if (node_arr.empty()) {
		parent->node_mutex.unlock();
		return Status(grpc::StatusCode::UNAVAILABLE, "No nodes");
	}
	int bucket_id = get_bucket_id(req->key(), parent->num_buckets);
	int node_arr_size = (int) node_arr.size();
	int start = bucket_id;
	int num_reps = parent->rep_factor;
	int count = 0; // this is the number of nodes we have gone through (we always want K nodes of data or try to because of the number of replicas)
					// we only disobey this if the number of nodes on our system is less than the number of replicas (bc nodes were killed)
	// When num_nodes_found == num_reps (iterated K times), terminate
	int num_nodes_found = 0;
	while (num_nodes_found < num_reps && count < node_arr_size) {
		int curr_idx = (start + count) % node_arr_size; // we start at "start" too because we want to count our current node as a "replica (K)"
		int curr_node_id = node_arr[curr_idx];
		// if the node is MARKED alive, do we want to add the node as a possible node for the key
		// first check if exists though
		if (parent->nodes.count(curr_node_id) && parent->nodes[curr_node_id].is_alive) {
			resp->add_replica_addrs(parent->nodes[curr_node_id].addr);
			resp->add_replica_ids(curr_node_id);
			num_nodes_found++;
		}
		count++;
	}
	parent->node_mutex.unlock();

	return Status::OK;
}

/**
 * This is to continuously poll ALL the nodes to check "liveness"
 * Based on if Manager receives a response for the particular node, if after 1 sec (assumed to be enough time for network call w/out being overly cautious)
 * 		... there is no response, then assumed to be dead, mark the node as "not alive"
 * Calls handle_node_failure() if node(s) failed
 */
void GTStoreManager::check_nodes() {
	while (true) {
		sleep(3);
		vector<NodeMeta> checked;
		node_mutex.lock();
		for (iterator = nodes.begin(); iterator != nodes.end(); iterator++) {
			checked.push_back(iterator->second);
		}
		node_mutex.unlock();
		for (int i = 0; i < (int) checked.size(); i++) {
			NodeMeta curr_node = checked[i];
			if (!curr_node.is_alive) {
				continue;
			}
			grpc::ClientContext context;
			// 1 second to respond --> w/in time window no response? ==> treated as dead
			context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
			gtstore::PingRequest req;
			gtstore::PingResponse resp;
			Status status = curr_node.stub->Ping(&context, req, &resp);
			if (!status.ok()) {
				node_mutex.lock();
				bool should_handle = false;

				auto it = nodes.find(curr_node.id);
				if (it != nodes.end() && it->second.is_alive) {
					it->second.is_alive = false;
					should_handle = true;
				}
				node_mutex.unlock();
				if (should_handle) {
					handle_node_failure(curr_node.id)
				}
			}
		}
	}
}

/**
 * Handler for dead nodes
 * Copies over its contents from some other node to a free node (not currently holding replica data for the node)
 * @param dead_node_id: id of the dead node
 */

void GTStoreManager::handle_node_failure(int dead_node_id) {
	cout << "Handling node failure on Node ID: " << dead_node_id << "\n";

	vector<int> nodes_arr;
	std::map<int, NodeMeta>::iterator iterator;
	node_mutex.lock();
	for (iterator = nodes.begin(); iterator != nodes.end(); iterator++) {
		nodes_arr.push_back(iterator->first);
	}
	node_mutex.unlock();

	for (int i = 0; i < rep_factor; i++) {
		// now we want to find the bucket we want to fix (bucket that stored replica data on the dead node)
		// 'i' represents the bucket BEFORE the dead node (because we store replicas from the 'primary node' up (by +1))
			// so Node 2 would store Node 1 replica data (in bucket 1)
		int bucket_to_fix = (dead_node_id - i + num_buckets) % num_buckets;
		int backup_id = -1;
		int target_id = -1;
		//cout << "Currently looking at bucket ID: " << bucket_to_fix << "'s data to rep\n";
		int total_nodes = (int) nodes.size();
		for (int j = 0; j < total_nodes; j++) {
			int idx = (bucket_to_fix + j) % total_nodes;
			int possible_id = nodes_arr[idx];
			if (nodes[possible_id].is_alive) {
				backup_id = possible_id; // this is the node we will copy data from to our future "target_id"
				break;
			}
		}
		int counter = 0; // we want to copy data over to the Kth node
		// now let's find a node that SHOULD be the new "replica" for curr data/node
		for (int j = 0; j < total_nodes; j++) {
			int idx = (bucket_to_fix + j) % total_nodes;
			int possible_id = nodes_arr[idx];
			if (nodes[possible_id].is_alive) {
				counter++;
				// kth alive node so this will be our new replica
				if (counter == rep_factor) {
					target_id = possible_id;
				}
			}
		}		
		if (backup_id == target_id) {
			//cout << "K=1 meaning no backup exists. Data is lost forever. Part of the project as mentioned by a Piazza post.\n";
			//return;
			//cout << "SKIPPED\n";
			continue;
		}
		
		if (backup_id != -1 && target_id != -1) {
			grpc::ClientContext context;
			gtstore::TransferDataRequest req;
			gtstore::TransferDataResponse resp;
			// let's get the addr of the target to tell the backup node to send data to this
			string target_addr = nodes[target_id].addr;
			req.set_dest_addr(target_addr);
			req.set_bucket_id(bucket_to_fix);

			// now actually do the work
			Status status = nodes[backup_id].stub->TransferData(&context, req, &resp);
			if (status.ok()) {
				cout << "Recovered lost data!!! Placed into NODE ID: " << target_id << "\n";
			} else {
				cout << "Recovery failed!!!\n";
			}
		} else {
			cout << "No backup or target node found\n";
		}
	}
}

/**
 * Takes in CML arguments and initializes the Manager to handle N nodes and K replicas
 * @param n: n nodes
 * @param k: k replicas
 */

void GTStoreManager::init(int n, int k) {
	cout << "Inside GTStoreManager::init()\n";
	cout << "Number of Nodes (N) = " << n << " Replication Factor (K) = " << k << "\n";
	this->num_buckets = n;
	this->rep_factor = k;
	string server_addr("0.0.0.0:50051"); // IP that "Manager" is hosted on
	ManagerService service(this);
	ServerBuilder builder;
	builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<Server> server(builder.BuildAndStart());

	cout << "We are listening on " << server_addr << "\n";
	monitor_thread = std::thread(&GTStoreManager::check_nodes, this);
	server->Wait();
}

int main(int argc, char **argv) {
	GTStoreManager manager;
	int n = DEF_BUCKET_COUNT;
	int k = DEF_REP;
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-n") == 0 || strcmp(argv[i], "--nodes") == 0) {
			if (i+1 < argc) {
				n = atoi(argv[i+1]);
				i++;
			}
		} else if (strcmp(argv[i], "-k") == 0 || strcmp(argv[i], "--rep") == 0) {
			if (i+1 < argc) {
				k = atoi(argv[i+1]);
				i++;
			}
		}
	}
	manager.init(n, k);
	return 0;
}