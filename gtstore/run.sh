#/bin/bash

make clean
make

pkill -9 manager
pkill -9 storage
pkill -9 test_app

# Launch the GTStore Manager
./bin/manager &
sleep 5

# Launch couple GTStore Storage Nodes
./bin/storage --port 50052 &
sleep 5
./bin/storage --port 50053 &
sleep 5

# Launch the client testing app
# Usage: ./test_app <test> <client_id>
./bin/test_app single_set_get 1 &
./bin/test_app single_set_get 2 &
./bin/test_app single_set_get 3

