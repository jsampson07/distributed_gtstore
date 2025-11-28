#!/bin/bash

MAN_PORT=50051
STORAGE_FIRST=50052
N=7
K=3
# let's cleanup our old processes from previous runs
./cleanup.sh
sleep 2 # just wait
./bin/manager -n $N -k $K &
echo "Manager started"
sleep 2 # again just wait
for ((i=0; i<N; i++)); do
    CURR_PORT=$((STORAGE_FIRST + i))
    ./bin/storage --port $CURR_PORT &
    echo "Storage Node ID: $i started, Port: $CURR_PORT"
    sleep 0.2
done
sleep 3
echo "Now let's populate our data"
for i in {1..20}; do
    ./bin/test_app --put key$i --val val$i
done

echo "Now let's overwrite some keys are told by project test description"
./bin/test_app --put key1 --val UPDATED_VAL_1
./bin/test_app --put key10 --val UPDATED_VAL_10
./bin/test_app --put key20 --val UPDATED_VAL_20

echo -e "Now kill 2 nodes\n"
pkill -9 -f "port 50053"
pkill -9 -f "port 50056"
sleep 6 # to play it safe let's wait 5 seconds to allow manager to detect the dead nodes before moving on

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