#include "gtstore.hpp"
#include <cstring>

/**
 * This method "registers" a Node within the system
 * Adds this to the "nodes" map to keep track of all the nodes initialized (NOT a "live" feed of 'live' nodes) --> this is the job of the running_nodes
 * @return a status after successfully adding a node
 */
Status GTStoreManager::ManagerService::Register(ServerContext *context, const gtstore::RegisterNodeRequest *req, gtstore::RegisterNodeResponse *resp) {
	parent->node_mutex.lock();
	int id = parent->next_node_id;
	parent->next_node_id++; // let's prepare for adding our next node next time
	NodeMeta new_node; // this is to set our new nodes information
	new_node.id = id; // get our id
	new_node.addr = req->address(); // the node that requested to be "Registered" 
	// this is to create the connection channel
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(new_node.addr, grpc::InsecureChannelCredentials());
	// now let's create the stub, the connection we will use for the manager to communicate with the node
	new_node.stub = gtstore::StorageService::NewStub(channel);
	parent->nodes[id] = new_node; // let's add this node to the map of nodes we have for our system
	// now the manager provides the response so we need to set its fields which we defined in the .proto file
	new_node.is_alive = true; // now that we are creating the node it is alive (setting this as LATE as possible incase any errors occur)
	// set response fields so Node gets info it needs and can use when returns in it's "init" method
	resp->set_node_id(id);
	resp->set_success(new_node.is_alive);
	resp->set_part_count(parent->num_parts);
	parent->node_mutex.unlock();
	cout << "Manager has responded to Register request, created new node ID: " << new_node.id << " Address: " << new_node.addr << "\n";
	return Status::OK;
}

/**
 * This gets/finds node(s) that hold the data corresponding to a key
 * This is used for "PUTTING" data and also "getting" data from the client ==> in return we get a list of addresses that we can query
 * 	==> replication --> utilize replication to also help LOAD BALANCING
 * @return a status of successful response or not
 * 
 * 






 LOOOK AT THIS FUNCTION!!!!!!!!!!!!!!!!!




 */
Status GTStoreManager::ManagerService::GetNodeForKey(ServerContext *context, const gtstore::GetNodeForKeyRequest *req, gtstore::GetNodeForKeyResponse *resp) {
	parent->node_mutex.lock();
	vector<int> running_nodes; // this is to get ONLY the nodes which are "active"
	std::map<int, NodeMeta>::iterator iterator;
	// now let's iterate and get only those nodes which are "alive" according to the "Manager"
	for (iterator = parent->nodes.begin(); iterator != parent->nodes.end(); iterator++) {
		if (iterator->second.is_alive) {
			running_nodes.push_back(iterator->first);
		}
	}
	if (running_nodes.empty()) {
		parent->node_mutex.unlock();
		return Status(grpc::StatusCode::UNAVAILABLE, "None"); // is there something else I can return??????????
	}
	// this is to calculate where to place the data in the particular node
	int part_id = get_bucket_id(req->key(), parent->num_parts);
	int start = part_id % running_nodes.size(); // why do we need this line???
	
	

	// I DO NOT THINK WE NEED THIS SECTION BUT SURE!!!!!!!! FOR NOW! CHECK LATER
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
	return Status::OK;
}

/**
 * Continuously scans ALL nodes on the system to see if it is alive or not (every few seconds (~3))
 * IF IT IS NOT ALIVE, i.e. the storage node service sends no response, we mark the node as dead
 * handle_node_failure is called here if a node is dead
 */
void GTStoreManager::check_nodes() {
	// we want this to be infinite loop, always keep checking
	while (true) {
		sleep(3); // let's just check all nodes status's every 3 secs
		vector<NodeMeta> checked;
		node_mutex.lock(); // is this right???????
		std::map<int, NodeMeta>::iterator iterator;
		for (iterator = nodes.begin(); iterator != nodes.end(); iterator++) {
			checked.push_back(iterator->second);
		}
		node_mutex.unlock(); // is this right??????????
		for (size_t i = 0; i < checked.size(); i++) {
			NodeMeta node = checked[i];
			// the way i have designed this is the Manager only after realizing that the node is dead, sets this field to false
			// so.... if already false by the time we enter this loop, we already knew it was dead (the whole point is to detect NEW dead nodes)
			if (!node.is_alive) {
				continue;
			}
			grpc::ClientContext context;
			context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1)); // let's give the response a window of time before we consider it DEAD
			gtstore::PingRequest req;
			gtstore::PingResponse resp;
			Status status = node.stub->Ping(&context, req, &resp);
			if (!status.ok()) { // if we do not get an "OK" status, then we failed (dead)
				node_mutex.lock(); // IS THIS RIGHT????
				if (nodes[node.id].is_alive) {
					// NOW WE know that we have a dead node FOR SURE because it has been marked as alive but is not responding
					nodes[node.id].is_alive = false;
					handle_node_failure(node.id); // now we want to handle the node failure and replicate its data onto other node(s)
													// while also maintaining the "sharding" behavior
				}
				node_mutex.unlock(); // NOT SURE IF THIS SI RIGHT OR DO I NEED "parent"???
			}
		}
	}
}

/**
 * 
 */
void GTStoreManager::handle_node_failure(int dead_node_id) {
	cout << "Handling node failure on Node ID: " << dead_node_id << "\n";
	int partition_lost = dead_node_id % num_parts;
	int backup_id = -1;
	int target_id = -1;
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
	/* 1) node is alive
	   2) node is NOT the backup node that we just found */
	std::map<int, NodeMeta>::iterator iterator;
	for (iterator = nodes.begin(); iterator != nodes.end(); iterator++) {
		int maybe_target = iterator->first;
		NodeMeta info = iterator->second; // is this correct syntax?????????????????????????????????????????/
		if (info.is_alive && (maybe_target != backup_id) && (maybe_target != dead_node_id)) {
			target_id = maybe_target;
			break;
		}
	}

	// now we have to transfer the data the dead node had to the target node
	if (backup_id != -1 && target_id != -1) {
		// let's recover !
		grpc::ClientContext context;
		gtstore::TransferDataRequest req;
		gtstore::TransferDataResponse resp;
		// let's get the addr of the target to tell the backup node (with the replica data) to send data there
		string target_addr = nodes[target_id].addr;
		req.set_dest_addr(target_addr);
		req.set_partition_id(partition_lost);

		// now lets get the backup node (which contains the replica data of the dead node) to send data over to the target node
		Status status = nodes[backup_id].stub->TransferData(&context, req, &resp);
		if (status.ok()) {
			cout << "Recovered lost data!!!\n";
		} else {
			cout << "Recovery failed!!!: " << status.error_code() << "\n";
		}
	} else {
		cout << "No backup or target node found\n";
	}
}

void GTStoreManager::init(int n, int k) {
	cout << "Inside GTStoreManager::init()\n";
	cout << "N = " << n << " K = " << k << "\n";
	this->num_parts = n;
	this->rep_factor = k;
	string server_addr("0.0.0.0:50051"); // this is the IP that "Manager" is hosted on
	ManagerService service(this);
	ServerBuilder builder;
	builder.AddListeningPort(server_addr, grpc::InsecureServerCredentials()); // what is this doing??? INSECURESERVERCREDENTIALS??
	builder.RegisterService(&service);
	std::unique_ptr<Server> server(builder.BuildAndStart());

	cout << "We are listening on " << server_addr << "\n";
	monitor_thread = std::thread(&GTStoreManager::check_nodes, this);
	server->Wait();
}

int main(int argc, char **argv) {
	GTStoreManager manager;
	int n = DEF_PART_COUNT;
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
