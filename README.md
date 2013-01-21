lustreHarvest
=============

harvest Lustre filesystem OST/MDT read, write, iops stats and spoof them into the ganglia of client nodes.

the goal is to have i/o rates to [Lustre](http://en.wikipedia.org/wiki/Lustre_%28file_system%29 "Lustre at Wikipedia") filesystems show up in the [ganglia](https://github.com/ganglia/) of each cluster node so that we can easily see how much i/o each client is doing. lustreHarvest gathers read, write and iops rates from OSS machines and iops from MDS machines. Multiple filesystems on each OSS or MDS are supported.

lustreHarvest is a simple python script that acts as both data gatherer on the Lustre servers and as aggregator on the machine that spoofs the processed data into ganglia.

![alt text](http://sf.anu.edu.au/~rjh900/git/lustreHarvest/cluster_ops.png "whole cluster iops")
![alt text](http://sf.anu.edu.au/~rjh900/git/lustreHarvest/node_io.png "read and write i/o from one node")

How it Works
------------

data gathering occurs on Lustre OSS/MDS server nodes eg.

    lustreHarvest.py host home short

where ''host'' is the name of the machine to send data to, and ''home'' and ''short'' are the names of Lustre OSTs present.

the master aggregator process runs on ''host'' which is typically a management server. this needs no arguments. eg.

    lustreHarvest.py

this turns the data it recieves into rates for each client and spoofs these into [ganglia](https://github.com/ganglia/) using the [gmetric.py](https://github.com/ganglia/ganglia_contrib/tree/master/gmetric-python) module.

data is tranferred by sending serialised python objects transported over simple TCP connections. client sends are closely synchronised so that the server can tell when a data gathering sweep is finished, sum and generate statistics for each client, and spoof close to coherent data into ganglia. data integrity is verified by md5 sums of the objects. authenticity is ensured by using a shared secret.

lustreHarvest transparently handles client and server process disconnections and restarts (eg. OSS reboots).


Advanced Setup - Relaying and Site Wide Filesystems
---------------------------------------------------

a shared or site-wide filesystem is generally mounted on several clusters. the site-wide filesystem is assumed to know nothing about the compute nodes on each cluster, so the data is sent back using ''relaying''. once the site-wide filesystem data arrives at the head node of each cluster it is then spoofed into ganglia like usual, and then all compute nodes in every cluster will have ganglia entries for how much i/o they are doing to the site-wide filesystem.

probably the simplest way to setup relaying is to use a different TCP port and separate lustreHarvest daemons for the site-wide filesystem data.

for example, OSS and MDS's of the site-wide filesystem ''gdata'' will send to port 8023 on relaying (or site-wide head) node ''alkindi'' with

    lustreHarvest.py --port 8023 alkindi gdata

''alkindi'' listens with

    lustreHarvest.py --port 8023

and then config lines in the script (yes, I know these should really be in a config file) tell ''alkindi'' where to relay the data to. for example

```python
# names of cluster head nodes where server instances of this script run
head = { 'vu':'vu-man4', 'xe':'xepbs', 'dcc':'dccpbs' }

# lnets of each cluster
localLnets = { 'vu':'o2ib', 'xe':'o2ib2', 'dcc':'tcp102' }

# relaying servers send summed data to various other server instances.
# specify which clusters to relay to
relay = { 'alkindi': [ 'vu', 'xe', 'dcc' ] }
```

tells alkindi that that it will be relaying data to three other clusters, and which lnets are associated with which clusters.
eg. data gathered for the lnet ''o2ib2'' will be sent to head node ''xepbs'' on the ''xe'' cluster.

on the cluster head node ''xepbs'', another instance of lustreHarvest runs and listens for the relayed data on the external interface

    lustreHarvest.py --port 8023 --interface xepbs.anu.edu.au

and then spoofs this data into ganglia for all the ''xe'' compute nodes like usual.
