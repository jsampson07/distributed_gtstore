## Initialize Manager

```bash
# Option 1: Short flags
./bin/manager -n <num_of_nodes> -k <replication_factor>

# Option 2: Long flags
./bin/manager --nodes <num_of_nodes> --rep <replication_factor>
```

## Initialize Storage Node

```bash
./bin/storage --port <port_number>
```

## Run Test App

**Single Set/Get:**
```bash
./bin/test_app single_set_get <client_id>
```

**Put / Get Operations:**
```bash
# Put value
./bin/test_app --put <key> --val <value>

# Get value
./bin/test_app --get <key>
```

## Run All Tests

```bash
./run.sh
```

## Cleanup Processes

```bash
./cleanup.sh
```

## Run Test 4 Exclusively

```bash
./test4.sh
```