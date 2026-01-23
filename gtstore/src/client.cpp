#include "gtstore.hpp"

/**
 * This is to create a new channel if not found, otherwise let's use the same one we created before
 * This avoids having to create a new connection everytime we want perform some node behavior
 * @return pointer to the Stub
 */
gtstore::StorageService::Stub* GTStoreClient::get_node_stub(string addr) {
	// if the node_stub does NOT exist for this addr, we want to create it and create a new mapping for it
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
	// Establishes connetion with Manager to allow for communication with the system
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(man_addr, grpc::InsecureChannelCredentials());
	manager_stub = gtstore::ManagerService::NewStub(channel);
	
	// Seed for random function
	srand(time(0));

	cout << "Client initialized. We have successfully connected to the Manager!\n";
}

/**
 * Here we just want to retrieve the data
 * Utilize GetNodeForKey
 * @return the value
 */
val_t GTStoreClient::get(string key) {
	cout << "Inside GTStoreClient::get() for client: " << client_id << " key: " << key << "\n";
	val_t value;
	grpc::ClientContext context;
	gtstore::GetNodeForKeyRequest req;
	gtstore::GetNodeForKeyResponse resp;
	
	req.set_key(key);
	// Retrieves nodes to query for key
	context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
	Status status = manager_stub->GetNodeForKey(&context, req, &resp);
	if (!status.ok()) {
		cout << "Failed to talk to Manager\n";
		return value;
	}
	int num_reps = resp.replica_addrs_size();
	// pick a "random" starting index (READ LOAD BALANCING)
	int start_idx = rand() % num_reps;
	for (int i = 0; i < num_reps; i++) {
		int curr_idx = (start_idx + i) % num_reps;
		string addr = resp.replica_addrs(curr_idx);
		grpc::ClientContext context2;
		gtstore::GetRequest req2;
		gtstore::GetResponse resp2;
		req2.set_key(key);
		gtstore::StorageService::Stub *stub = get_node_stub(addr);

		context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
		Status status = stub->Get(&context2, req2, &resp2);

		if (status.ok() && resp2.found()) {
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
	// Value will be empty if no data was found
	return value;
}

/**
 * For this method, we want to write the data to the "primary" node AND ALL "K" replicas
 * NOTE: from piazza post ==> makes sense but if K=1, then it should be impossible to retrieve data when "the" node dies because we only copy data into K nodes (1 here)
 * NOTE FOR CLARIFICATION: NOT K+1 nodes, rather we write to K nodes total (the data is stored on K nodes TOTAL)
 */
bool GTStoreClient::put(string key, val_t value) {
	if (key.length() > MAX_KEY_BYTE_PER_REQUEST) {
		cout << "Key is too large!!! Max is 20 chars/bytes.\n";
		return false;
	}
	string print_value = "";
	int total_val_size = 0;
	for (uint i = 0; i < value.size(); i++) {
		print_value += value[i] + " ";
		total_val_size += value[i].length();
	}
	if (total_val_size > MAX_VALUE_BYTE_PER_REQUEST) {
		cout << "Value is too large!!! Max is 1KB.\n";
		return false;
	}
	
	cout << "Inside GTStoreClient::put() for client: " << client_id << " key: " << key << " value: " << print_value << "\n";

	// Fetch the current val before overwriting so we can undo our changes (incomplete write operation)
	val_t old_value = this->get(key);

	grpc::ClientContext context;
	gtstore::GetNodeForKeyRequest req;
	gtstore::GetNodeForKeyResponse resp;
	req.set_key(key);

	context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
	Status status = manager_stub->GetNodeForKey(&context, req, &resp);

	if (!status.ok()) {
		//cout << "UHOH We have failed to connect to the Manager !!!!\n";
		return false;
	}

	int success_count = 0;
	// Var for printing ONLY (string of addresses successful write to)
	string servers = "";

	// Track nodes that need rolling back on a write failure (revert back to "old_value")
	vector<string> successful_addrs;

	for (int i = 0; i < resp.replica_addrs_size(); i++) {
		string addr = resp.replica_addrs(i);
		grpc::ClientContext context2;
		gtstore::PutRequest req2;
		gtstore::PutResponse resp2;
		req2.set_key(key);
		// To store value want to write it to proto buff so...
		convert_to_protobuf(value, req2.mutable_value());

		gtstore::StorageService::Stub *stub = get_node_stub(addr);

		context2.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
		Status status = stub->Put(&context2, req2, &resp2);

		if (status.ok()) {
			success_count++;
			servers += addr + "  ";
			// We log this node as successfully written to (potential for needing rollback) if future failure
			successful_addrs.push_back(addr);
		}
	}
	if (success_count == resp.replica_addrs_size()) { // Expected to be equal if all writes succeeded
		//cout << "We have successfully written all data for client\n";
		//cout << "We have written data to: " << success_count << " nodes.\n";
		cout << "> OK, " << servers << "\n";
		return true;
	} else {
		cout << "We wrote to" << success_count << "... BUT Expected to write to :" << resp.replica_addrs_size() << "\n";
		cout << "Initiating Rollback to previous state...\n";

		for (string rollback_addr : successful_addrs) {
			grpc::ClientContext rollback_ctxt;
			gtstore::PutRequest rollback_req;
			gtstore::PutResponse rollback_resp;

			rollback_req.set_key(key);
			// Prepare to write old value to original value for key
			convert_to_protobuf(old_value, rollback_req.mutable_value());
			gtstore::StorageService::Stub *rollback_stub = get_node_stub(rollback_addr);

			rollback_ctxt.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(1));
			Status rollback_status = rollback_stub->Put(&rollback_ctxt, rollback_req, &rollback_resp);
			if (rollback_status.ok()) {
				cout << "Successfully rolled back node: " << rollback_addr << "\n";
			} else {
				cout << "Rollback failed on node: " << rollback_addr << "\n";
			}
		}
		return false;
	}
}

void GTStoreClient::finalize() {
	cout << "Inside GTStoreClient::finalize() for client " << client_id << "\n";
}