# To initialize manager
./bin/manager -n <num_of_nodes> -k <replication_factor>
OR
./bin/manager --nodes <num_of_nodes> --rep <replication_factor>

# To initialize storage node
./bin/storage --port <port_number>

# To run test_app.cpp (allows to run provided single_set_get)
./bin/test_app single_set_get <client_id>

# To run test_app.cpp (to just "put" or "get" values)
**PUT**
./bin/test_app --put <key> --val <value>
**GET**
./bin/test_app --get <key>

# To run the run script (contains ALL tests (test1-4 AND performance tests))
./run.sh

# To cleanup processes
./cleanup.sh

# To run test4 exclusively (which was done in personal testing)
./test4.sh