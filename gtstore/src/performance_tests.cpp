#include "gtstore.hpp"
#include <chrono>
#include <vector>
#include <map>
#include <iomanip>

using namespace std;

void run_throughput_test(int client_id, int num_ops) {
    cout << "========================================\n";
    cout << "Starting THROUGHPUT test\n";
    cout << "========================================\n";

    GTStoreClient client;
    client.init(client_id);

    // random payload haha
    string payload = "abcdefghijklmnopqrstuvwxyz1234567891011121314151617181920ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()";
    vector<string> value_vec;
    value_vec.push_back(payload);

    auto start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_ops; i++) {
        string key = "key_" + to_string(i); // now let's create a unique key --> we knwo i will change use as uniqueness
        if (i % 2 == 0) { // this is how we write 1/2 reads 1/2 writes (every other read/write)
            client.put(key, value_vec);
        } else { // here we read (get)
            int prev_idx = i-1; // we use prev_idx to read data we know is there and actually just wrote
                                // we know it will be there bc we will not resume from previous .put() until it returns successfully!!!
            string read_key = "key_" + to_string(prev_idx);
            client.get(read_key);
        }
        if (i % 10000 == 0 && i > 0) { // for TA's to reassure not frozen terminal ==> let's print progress every ~10000 ops, should be frequent-ish updates
            cout << "Completed " << i << " operations\n";
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> time_elapsed = end_time - start_time;
    double seconds = time_elapsed.count();
    
    double throughput = num_ops / seconds; // this is the calculation we care about (Ops/sec) --> will plot over num of replicas

    cout << "\n========================================\n";
    cout << "Total time ran: " << seconds << " secs\n";
    cout << "Throughput: " << throughput << " Ops/sec\n";
    cout << "==========================================\n";

    client.finalize();
}

void run_load_balance_test(int client_id, int num_ops, int num_nodes) {
    cout << "========================================\n";
    cout << "Starting LOAD BALANCE test\n";
    cout << "Number of nodes: " << num_nodes << "\n";
    cout << "========================================\n";

    GTStoreClient client;
    client.init(client_id);

    map<int, int> key_counts; // this is a map to keep count of number of keys that are on each node (CHECK LOAD BALANCING!!!)
                                // <node ID, # of keys>
    for(int i = 0; i < num_nodes; i++) { // initialize count of keys on each node to 0
        key_counts[i] = 0;
    }

    string payload = "abcdefghijklmnopqrstuvwxyz1234567891011121314151617181920ABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%^&*()";
    vector<string> value_vec;
    value_vec.push_back(payload);

    for (int i = 0; i < num_ops; i++) {
        string key = "key_" + to_string(i);
        client.put(key, value_vec);
        // now to see which node the key hashed to we can just use the get_bucket_id function
        // bucket # <==> node id
        int target_node = get_bucket_id(key, num_nodes);
        key_counts[target_node]++; // we increase the key count for the node and we keep doing this

        if (i % 10000 == 0 && i > 0) { // this is for TAs convenience
            cout << "Inserted " << i << " keys so far\n";
        }
    }
    cout << "\nHISTOGRAM\n";
    cout << "Node ID | Key Count | Distribution\n";
    cout << "--------|-----------|-------------\n";

    // this is to print the histogram
    int scale = 500;
    for (int i = 0; i < num_nodes; i++) {
        int count = key_counts[i];
        int num_chars = count / scale;
        string bar(num_chars, '#');
        cout << setw(7) << i << " | " 
             << setw(9) << count << " | "
             << bar << "\n";
    }
    client.finalize();
}

int main(int argc, char** argv) {
    // Basic check for command line arguments
    if (argc < 2) {
        cout << "Please write the commands as follows: \n";
        cout << "  ./bin/perf_test throughput <num_nodes>\n";
        cout << "  ./bin/perf_test loadbalance <num_nodes>\n";
        return 1;
    }
    string mode = argv[1];
    int num_nodes = 7; // Default to 7 nodes as per spec

    if (mode == "throughput") { // Run throughput test with 200,000 operations
        run_throughput_test(420, 200000); 
    } else if (mode == "loadbalance") { // Run load balance test with 100,000 inserts
        run_load_balance_test(67, 100000, num_nodes); 
    } else {
        cout << "Unknown mode: " << mode << endl;
        return 1;
    }
    return 0;
}