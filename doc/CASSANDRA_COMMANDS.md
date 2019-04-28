# Commands for Running Cassandra
Here is a list of commands used as a reference for running YCSB workloads against Cassandra and collecting traces.

#### Loading Phase
The following is an example of the loading phase loading workload A data:
```
./bin/ycsb load cassandra-cql -p hosts=localhost -p recordcount=1000000  -p workloads/workloada -s > a_load_1.txt
```
#### Clear Page Cache
```
echo 3 > /proc/sys/vm/drop_caches
```
#### Transaction Phase
The following is an example of the transaction phase running workload A:
```
./bin/ycsb run cassandra-cql -p hosts=localhost -P workloads/workloada -p recordcount=1000000 -p operationcount=1000000 > a_run_1.txt
```
#### Blktrace command
This command should be executed right before the transaction phase in order to collect all the transactions. The following is an 
example of writing to the workload A output file using the ext4 file system.
```
sudo blktrace -d /dev/sdb -w 30000 -o - | blkparse -a fs -i - > workloada_trace_ext4.txt
```
