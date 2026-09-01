#!/bin/bash

MAN_PORT=50051
STORAGE_FIRST=50052
N=7 # 7 storage nodes
K=3 # Replication factor of 3

# cleanup any old processes from previous runs
./cleanup.sh
sleep 2 # wait for cleanup to complete

# start the manager with 7 nodes and rep factor = 3
./bin/manager -n $N -k $K &
echo "Manager started"
sleep 2 # wait for manager to start

# Start the 7 storage nodes
for ((i=0; i<N; i++)); do
    CURR_PORT=$((STORAGE_FIRST + i))
    ./bin/storage --port $CURR_PORT &
    echo "Storage Node ID: $i started, Port: $CURR_PORT"
    sleep 0.2 # wait for each node to register with manager
done
sleep 3
echo "Now let's populate our data"

# Populate with 20 key-value pairs
for i in {1..20}; do
    ./bin/test_app --put key$i --val val$i
done

echo "Now let's overwrite some keys are told by project test description"

# I overwrote the values of 3 keys, with clear UPDATED values
./bin/test_app --put key1 --val UPDATED_VAL_1
./bin/test_app --put key10 --val UPDATED_VAL_10
./bin/test_app --put key20 --val UPDATED_VAL_20

echo -e "Now kill 2 nodes\n"

# Killed 2 storage nodes
pkill -9 -f "port 50053"

sleep 4 # to play it safe let's wait 5 seconds to allow manager to detect the dead nodes before moving on

pkill -9 -f "port 50056"

sleep 10

echo "NOTE: expected key-value pairs are as follows..."
echo "<key1, val1>, <key2, va2>, <key3, val3>, ..., <key20, val20>"
echo "WITH THREE EXCEPTIONS:"
echo -e "<key1, UPDATED_VAL_1>, <key10, UPDATED_VAL_10>, <key20, UPDATED_VAL_20>\n"
./bin/test_app --get "key1"
./bin/test_app --get "key10"
./bin/test_app --get "key20"
./bin/test_app --get "key3"
./bin/test_app --get "key9"
./bin/test_app --get "key17"

echo "TEST 4 DONE!!!"
./cleanup.sh