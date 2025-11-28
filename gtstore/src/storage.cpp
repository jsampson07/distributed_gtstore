#include "gtstore.hpp"

/**
 * Puts the key value pair into the bucket's "data_map"
 * @return Status
 */
Status GTStoreStorage::StorageService::Put(ServerContext *context, const gtstore::PutRequest *req, gtstore::PutResponse *resp) {
	if (parent->num_buckets == 0) {
		return Status(grpc::StatusCode::FAILED_PRECONDITION, "We have no buckets");
	}
	string key = req->key(); // get the key value
	int bucket_id = get_bucket_id(key, parent->num_buckets);
	Bucket *bucket = parent->buckets[bucket_id].get();
	// now we will write the data to this bucket
	val_t value = convert_from_protobuf(req->value()); 
	bucket->bucket_mutex.lock();
	// now that we have the key and value from the request, let's add it to our respective buckets map
	bucket->data_map[key] = value;
	resp->set_success(true);
	bucket->bucket_mutex.unlock();
	return Status::OK;
}

/**
 * Retrieves the value from the key provided
 * @return Status
 */
Status GTStoreStorage::StorageService::Get(ServerContext *context, const gtstore::GetRequest *req, gtstore::GetResponse *resp) {
	if (parent->num_buckets == 0) {
		return Status(grpc::StatusCode::FAILED_PRECONDITION, "We have no buckets");
	}
	string key = req->key(); // the key from the request (this is the infomration that the Client wants to get)
	int bucket_id = get_bucket_id(key, parent->num_buckets);
	Bucket *bucket = parent->buckets[bucket_id].get(); // get the data
	// now that we are accessing data from the partition, we do not want to be able to write to it right now
	bucket->bucket_mutex.lock();
	if (bucket->data_map.find(key) != bucket->data_map.end()) {
		// now lets set the fields in the response for client
		resp->set_key(key);
		convert_to_protobuf(bucket->data_map[key], resp->mutable_value());
		resp->set_found(true);
	} else {
		resp->set_found(false); // we did NOT find anything/the key does not exist !!!
	}
	bucket->bucket_mutex.unlock();
	return Status::OK;
}

/**
 * Used to transfer the data from one node to the intended ("target") node
 * @return Status
 */
Status GTStoreStorage::StorageService::TransferData(ServerContext* context, const gtstore::TransferDataRequest *req, gtstore::TransferDataResponse *resp) {
	string target = req->dest_addr(); // this is to get the destination address passed in by the request
	int bucket_id = req->bucket_id();
	// we are creating a new channel to communicate with the node i will transfer the data over to
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(target, grpc::InsecureChannelCredentials());
	std::unique_ptr<gtstore::StorageService::Stub> stub = gtstore::StorageService::NewStub(channel);
	Bucket *bucket = parent->buckets[bucket_id].get();

	// let's acquire the lock now so that no one else can access this partition while we are copying data over
	bucket->bucket_mutex.lock();
	std::unordered_map<string, val_t>::iterator iterator;
	for (iterator = bucket->data_map.begin(); iterator != bucket->data_map.end(); iterator++) {
		grpc::ClientContext context;
		gtstore::PutRequest req;
		gtstore::PutResponse resp;
		req.set_key(iterator->first); // note that ->first is the key and ->second is the value
		convert_to_protobuf(iterator->second, req.mutable_value()); // turn into format for protocol buffer

		Status status = stub->Put(&context, req, &resp);
	}
	bucket->bucket_mutex.unlock();
	//cout << "We have successfully transfered all the data\n";
	resp->set_success(true);
	return Status::OK;
}

// this is all the storage node has to do when responding to a Ping request from the Manager (respond to server) --> acknowledge!
Status GTStoreStorage::StorageService::Ping(ServerContext *context, const gtstore::PingRequest *req, gtstore::PingResponse *resp) {
	resp->set_ack(true);
	return Status::OK;
}

void GTStoreStorage::init(int port) {
	cout << "Inside GTStoreStorage::init()\n";
	my_addr = "0.0.0.0:" + to_string(port);
	man_addr = "0.0.0.0:50051";
	StorageService service(this);
	grpc::ServerBuilder builder;
	builder.AddListeningPort(my_addr, grpc::InsecureServerCredentials());
	builder.RegisterService(&service);
	std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
	if (!server) {
		cout << "Failed to bind to address: " << my_addr << "\n";
		exit(1);
	}
	std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(man_addr, grpc::InsecureChannelCredentials());
	manager_stub = gtstore::ManagerService::NewStub(channel);
	grpc::ClientContext context;
	gtstore::RegisterNodeRequest req;
	gtstore::RegisterNodeResponse resp;
	req.set_address(my_addr);

	Status status = manager_stub->Register(&context, req, &resp);
	if (!status.ok()) {
		cout << "WE FAILED to Register Node! Womp Womp\n";
	}
	node_id = resp.node_id();
	num_buckets = resp.bucket_count();
	// now let's initialize the buckets with Partition's
	for (int i = 0; i < num_buckets; i++) {
		buckets.push_back(unique_ptr<Bucket>(new Bucket()));
	}
	cout << "Successfully registered node ID: " << node_id << "\n";
	cout << "Number of buckets: " << num_buckets << "\n";
	server->Wait();
}

int main(int argc, char **argv) {
	GTStoreStorage storage;
	int port = 50052; // lets just default this to +1 of the manager
	for (int i =1; i < argc; i++) {
		if (string(argv[i]) == "--port") {
			if (i+1 < argc) {
				port = atoi(argv[i+1]);
				i++;
			}
		}
	}
	storage.init(port);
}
