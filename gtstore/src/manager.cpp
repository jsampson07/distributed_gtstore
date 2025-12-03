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
	// this is to create the connection channel
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(new_node.addr, grpc::InsecureChannelCredentials());
	// now let's create the stub, the connection we will use for the manager to communicate with the node
	new_node.stub = gtstore::StorageService::NewStub(channel);
	parent->nodes[id] = new_node; // let's add this node to the map of nodes we have for our system
	// now the manager provides the response so we need to set its fields which we defined in the .proto file
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
	int bucket_id = get_bucket_id(req->key(), parent->num_buckets); // this is to calculate correct bucket corresponding to the key hashed
	int node_arr_size = (int) node_arr.size();
	int start = bucket_id;
	int num_reps = parent->rep_factor;
	int count = 0; // this is the number of nodes we have gone through (we always want K nodes of data or try to because of the number of replicas)
					// we only disobey this if the number of nodes on our system is less than the number of replicas (bc nodes were killed)
	int num_nodes_found = 0; // once this is num_reps then we know we have iterated our "K" factor of times
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
	if (count == 0) {
		return Status(grpc::StatusCode::UNAVAILABLE, "There are no nodes in the system...");
	}
	
	/* OLD CODE SNIPPET
	// now we want K copies but what if we only have < K nodes right now??? i.e. on initialization
	int needed;
	if (parent->rep_factor > (int) running_nodes.size()) {
		needed = (int) running_nodes.size();
	} else {
		needed = parent->rep_factor;
	}
	
	
	// now we want to select these "K" nodes from our start
	// let's write to the primary and the "Backup" nodes
	for (int i = 0; i < needed; i++) {
		int idx = (start + i) % running_nodes.size(); // this is for "wrap-around" behavior
		int curr_node = running_nodes[idx];
		resp->add_replica_addrs(parent->nodes[curr_node].addr);
		resp->add_replica_ids(curr_node);
	}
	parent->node_mutex.unlock();
	*/



	return Status::OK;
}

/**
 * This is to continuously poll ALL the nodes to check "liveness"
 * Based on if Manager receives a response for the particular node, if after 1 sec (assumed to be enough time for network call w/out being overly cautious)
 * 		... there is no response, then assumed to be dead, mark the node as "not alive"
 * Calls handle_node_failure() if node(s) failed
 */
void GTStoreManager::check_nodes() {
	// we want this to be infinite loop, always keep checking
	while (true) {
		sleep(3); // let's just check all nodes status's every 3 secs
		vector<NodeMeta> checked;
		node_mutex.lock(); // we want to make sure no one else can change "nodes" DS while iterating through
		std::map<int, NodeMeta>::iterator iterator;
		for (iterator = nodes.begin(); iterator != nodes.end(); iterator++) {
			checked.push_back(iterator->second);
		}
		node_mutex.unlock();
		for (int i = 0; i < (int) checked.size(); i++) {
			NodeMeta curr_node = checked[i];
			// the way this is designed is the  Manager only after realizing that the node is dead, sets this field to false
			// so.... if already false by the time we enter this loop, we already knew it was dead
			if (!curr_node.is_alive) {
				continue;
			}
			grpc::ClientContext context;
			// let's give it 1 second to respond (more than enough time) --> if not within a second --> assumed to be a DEAD node
			context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
			gtstore::PingRequest req;
			gtstore::PingResponse resp;
			Status status = curr_node.stub->Ping(&context, req, &resp);
			if (!status.ok()) { // if we do not get an "OK" status, then we failed (dead)
				if (nodes[curr_node.id].is_alive) {
					nodes[curr_node.id].is_alive = false;
					handle_node_failure(curr_node.id); // now we want to handle the node failure and replicate its data onto other node(s)
				}
			}
		}
	}
}

void GTStoreManager::handle_node_failure(int dead_node_id) {
	cout << "Handling node failure on Node ID: " << dead_node_id << "\n";
	//int backup_id = -1;
	//int target_id = -1;

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
			// MUST be alive 
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

		// bc of the above logic, this will not work when we have less nodes than our replication factor (K) data as mentioned before
		
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



	/*
	int total_nodes = nodes.size();
	for (int i = 0; i < total_nodes; i++) {
		int idx = (dead_node_id + i) % total_nodes;
		int possible_id = nodes_arr[idx];
		// MUST be alive and canNOT be the dead NODE!!!!
		if (nodes[possible_id].is_alive && possible_id != dead_node_id) {
			backup_id = possible_id;
			break;
		}
	}
	*/


	/*
	int total_nodes = nodes.size();
	// here with the design, the replica data will be in the next Node
	for (int i = 1; i < total_nodes; i++) {
		int replica_id = (dead_node_id + i) % total_nodes;
		// we are not guaranteed that all nodes will exist, but we want to initialize them all at first
		if (nodes.count(replica_id) && nodes[replica_id].is_alive) {
			backup_id = replica_id;
			break;
		}
	}

	// now we want to find a target that we can place the dead node's data into
	// what do we need to check for?

	*/


	/*
	int counter = 0;
	for (int i = 1; i < (int) total_nodes; i++) {
		int idx = (dead_node_id + i) % total_nodes;
		int possible_id = nodes_arr[idx];
		if (nodes[possible_id].is_alive) {
			counter++;
			if (counter == 1) {
				// this is going to be the backup (where we have the replica data)
				backup_id = possible_id;
			}
			if (counter == rep_factor) {
				target_id = possible_id;
			}
		}
	}
	*/

	/*
	if (backup_id == target_id) { // this is to check if our rep factor is 1, in which case the backup and target would be the same
		cout << "K=1 meaning no backup exists. Data is lost forever. Part of the project as mentioned by a Piazza post.\n";
		return;
	}
	*/


	/* 1) node is alive
	   2) node is NOT the backup node that we just found */

	/*
	std::map<int, NodeMeta>::iterator iterator2;
	for (iterator2 = nodes.begin(); iterator2 != nodes.end(); iterator2++) {
		int maybe_target = iterator2->first;
		NodeMeta curr_node = iterator2->second;
		if (curr_node.is_alive && (maybe_target != backup_id) && (maybe_target != dead_node_id)) {
			target_id = maybe_target;
			break;
		}
	}
	*/
}

void GTStoreManager::init(int n, int k) {
	cout << "Inside GTStoreManager::init()\n";
	cout << "Number of Nodes (N) = " << n << " Replication Factor (K) = " << k << "\n";
	this->num_buckets = n;
	this->rep_factor = k;
	string server_addr("0.0.0.0:50051"); // this is the IP that "Manager" is hosted on
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
