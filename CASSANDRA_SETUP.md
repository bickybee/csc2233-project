# Cassandra and YCSB Setup:
The following outlines the general setup and operation of Cassandra for the YCSB workloads.
#### Installation
Installation instructions for Cassandra and YCSB can be found here: 

http://cassandra.apache.org/doc/latest/getting_started/installing.html<br />
https://github.com/brianfrankcooper/YCSB

#### Keyspace
Cassandra requires a keyspace to define data replication on nodes. Even though we are only running one node, a keyspace is still 
required. The keyspace needed to run the YCSB workloads can be found here:<br /><br />
https://github.com/brianfrankcooper/YCSB/tree/master/cassandra

#### Create FileSystem
SSD is emulated by changing the rotational value from '1' to '0':
```
echo “0” > /sys/dev/block/8:16/queue/rotational
```
To mount a new filesystem to the workload disk, the existing FS is unmounted if it exists:
```
sudo umount -l /mnt 
```
The workload disk is then wiped:
```
wipefs -a /dev/sdb
```
The new file system is created (ex: ext4):
```
sudo mkfs.ext4 /dev/sdb 
```
The newly created file system is then mounted to the workload disk:
```
sudo mount /dev/sdb /media/mnt/
```

#### Cassandra Configuration
Cassandra requires four directories: the data file, commit log, hints, and saved caches directories. These four directories need to be created in the workload disk:
```
mkdir /mnt/media/data; 
mkdir /mnt/media/commitlog; 
mkdir /mnt/media/hints; 
mkdir /mnt/media/saved_caches
```
Cassandra then needs to be given access to these directories in the workload disk:
```
sudo chown -R cassandra:cassandra /media/mnt/data; 
sudo chown -R cassandra:cassandra /media/mnt/commitlog; 
sudo chown -R cassandra:cassandra /media/mnt/hints; 
sudo chown -R cassandra:cassandra /media/mnt/saved_caches
```
Cassandra needs to point to these new directories, which can configured in the cassandra.yaml file.


#### Running Cassandra
Cassandra can be started using the following command:
```
sudo service cassandra start
```
You should expect an output similar to the following:
```
● cassandra.service - LSB: distributed storage system for structured data
   Loaded: loaded (/etc/init.d/cassandra; generated)
   Active: active (running) since Sun 2019-04-28 12:02:24 EDT; 1s ago
     Docs: man:systemd-sysv-generator(8)
  Process: 3262 ExecStop=/etc/init.d/cassandra stop (code=exited, status=0/SUCCESS)
  Process: 3298 ExecStart=/etc/init.d/cassandra start (code=exited, status=0/SUCCESS)
```

