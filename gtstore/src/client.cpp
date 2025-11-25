#include "gtstore.hpp"

// WHAT IS THIS FUNCTION DOING?????????? WHAT AM I STORING?????????????
gtstore::StorageService::Stub* GTStoreClient::get_node_stub(string addr) {
	if (node_stubs.find(addr) == node_stubs.end()) {
		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
		node_stubs[addr] = gtstore::StorageService::NewStub(channel);
	}
	return node_stubs[addr].get();
}

void GTStoreClient::init(int id) {
		cout << "Inside GTStoreClient::init() for client " << id << "\n";
		client_id = id;
		string man_addr = "0.0.0.0:50051";
		// now all we need to do is establish a connection with the Manager
		std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(man_addr, grpc::InsecureChannelCredentials());
		manager_stub = gtstore::ManagerService::NewStub(channel);
		
		srand(time(0)); // this is to make rand() "random" now

		cout << "Client initialized. We have successfully connected to the Manager!\n";
}

val_t GTStoreClient::get(string key) {
		cout << "Inside GTStoreClient::get() for client: " << client_id << " key: " << key << "\n";
		val_t value;
		// Get the value!!!!!!!!!!!
		grpc::ClientContext context;
		gtstore::GetNodeForKeyRequest req;
		gtstore::GetNodeForKeyResponse resp;
		
		req.set_key(key);
		// let's make the request to the manager to see which node to query for the key
		Status status = manager_stub->GetNodeForKey(&context, req, &resp);
		if (!status.ok()) {
			cout << "Failed to talk to Manager\n";
			return value;
		}
		int num_reps = resp.replica_addrs_size();
		// now after we get the number of nodes that we have stored data on let's pick a "random" starting index
		int start_idx = rand() % num_reps;
		// this generates a random starting index which we can start looping from (implemented with the "wrap-around" behavior)
		for (int i = 0; i < num_reps; i++) {
			int curr_idx = (start_idx + i) % num_reps;
			string addr = resp.replica_addrs(curr_idx);
			grpc::ClientContext context2;
			gtstore::GetRequest req2;
			gtstore::GetResponse resp2;
			req2.set_key(key);
			// now we want to initiate the Get communication
			gtstore::StorageService::Stub *stub = get_node_stub(addr);
			Status status = stub->Get(&context2, req2, &resp2);
			if (status.ok() && resp2.found()) { // is there ANOTHER way to do instead of .found() ==> WHERE DID .found() come from???????!!!!!!!
				value = convert_from_protobuf(resp2.value());
				string print_val = "";
				if (!value.empty()) {
					print_val = value[0];
				}
				cout << "> " << key << ", " << print_val << ", " << addr << "\n";
				return value;
			}
		}
		cout << "> " << key << ", (NONE)\n";
		return value; // this should be empty if nothing is found
}

/**
 * For this method, we want to write the data to the "primary" node AND ALL "K" replicas
 * NOTE: from piazza post ==> makes sense but if K=1, then it should be impossible to retrieve data because we only copy data into K nodes
 * NOTE FOR CLARIFICATION: NOT K+1 nodes, rather we write to K nodes total (the data is stored on K nodes TOTAL)
 */
bool GTStoreClient::put(string key, val_t value) {

		string print_value = "";
		for (uint i = 0; i < value.size(); i++) {
				print_value += value[i] + " ";
		}
		cout << "Inside GTStoreClient::put() for client: " << client_id << " key: " << key << " value: " << print_value << "\n";
		// Put the value!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
		grpc::ClientContext context;
		gtstore::GetNodeForKeyRequest req;
		gtstore::GetNodeForKeyResponse resp;
		// so we have a key, we need to know waht node to put this into
		req.set_key(key);
		// let's make a request to Manager to figure out what node(s) we can query
		Status status = manager_stub->GetNodeForKey(&context, req, &resp);
		if (!status.ok()) {
			cout << "UHOH We have failed to connect to the Manager !!!!\n";
			return false;
		}
		int success_count = 0;
		// this variable is for PRINTING PURPOSES FOR test results (we want to save the server addr)
		string server_addr = "";
		// now let's loop through K nodes
		for (int i = 0; i < resp.replica_addrs_size(); i++) {
			string addr = resp.replica_addrs(i);
			grpc::ClientContext context2;
			gtstore::PutRequest req2;
			gtstore::PutResponse resp2;
			req2.set_key(key);
			// again when we store value we want to write it to proto buff so...
			convert_to_protobuf(value, req2.mutable_value());

			gtstore::StorageService::Stub *stub = get_node_stub(addr); // IS THIS CORRECT? what is this FUNCTION DOING????????!!!!!!!!1
			Status status = stub->Put(&context2, req2, &resp2);
			if (status.ok()) {
				success_count++;
				server_addr = addr;
			}
		}
		if (success_count == resp.replica_addrs_size()) { // if our success count is K (as expected if all writes succeed)
			//cout << "We have successfully written all data for client\n";
			//cout << "We have written data to: " << success_count << " nodes.\n";
			cout << "> OK, " << server_addr << "\n";
			return true;
		} else {
			cout << "We wrote to" << success_count << "... BUT Expected to write to :" << resp.replica_addrs_size() << "\n";
			return false;
		}
}

void GTStoreClient::finalize() {
	cout << "Inside GTStoreClient::finalize() for client " << client_id << "\n";
	//node_stubs.clear();
}
