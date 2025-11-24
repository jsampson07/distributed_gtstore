#include "gtstore.hpp"

void single_set_get(int client_id) {
	cout << "Testing single set-get for GTStore by client " << client_id << ".\n";

	GTStoreClient client;
	client.init(client_id);

	string key = to_string(client_id);
	vector<string> value;
	value.push_back("phone");
	value.push_back("phone_case");

	client.put(key, value);
	client.get(key);

	client.finalize();
}


int main(int argc, char **argv) {
	string test = string(argv[1]);
	string test1 = "single_set_get";
	int client_id = atoi(argv[2]);
	if (argc >= 2 && test == test1) {
		single_set_get(client_id);
		return 0;
	}

	// THE BELOW IS FOR CUSTOM TESTING WITH MANUAL INPUTS INTO TERMINAL
	GTStoreClient client;
	client_id = rand();
	client.init(client_id);
	string command = "";
	string key = "";
	string value = "";
	for (int i = 1; i < argc; i++) {
		if (string(argv[i]) == "--put") {
			command = "put";
			if (i+1 < argc) {
				key = string(argv[i+1]);
				i++;
			}
		} else if (string(argv[i]) == "--get") {
			command = "get";
			if (i+1 < argc) {
				key = string(argv[i+1]);
				i++;
			}
		} else if (string(argv[i]) == "--val") {
			if (i+1 < argc) {
				value = string(argv[i+1]);
				i++;
			}
		}
	}
	// if we have a PUT command, then we should be using key and value
	if (command == "put") {
		if (key == "" || value == "") {
			cout << "Please provide correct parameters: ./bin/test_app --put <key> --val <value>\n";
		} else {
			vector<string> val;
			val.push_back(value);
			client.put(key, val);
		}
	} else if (command == "get") {
		if (key == "") {
			cout << "Please provide a key to retrieve: ./bin/test_app --get <key>\n";
		} else {
			client.get(key);
		}
	} else {
		cout << "The command you provided is NOT valid. Please write it in the following 2 ways:\n";
		cout << "For PUT requests: ./bin/test_app --put <key> --val <value>\n";
		cout << "For GET requests: ./bin/test_app --get <key>\n";
	}
	client.finalize();
}
