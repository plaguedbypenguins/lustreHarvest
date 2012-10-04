lustreHarvest
=============

harvest Lustre filesystem OST/MDT read, write, iops stats and spoof them into the ganglia of client nodes.

the goal is to have i/o rates to [Lustre](http://en.wikipedia.org/wiki/Lustre_(file_system&#41;) filesystems show up in the [ganglia](https://github.com/ganglia/) of each cluster node so that we can easily see how much i/o each client is doing. lustreHarvest gathers read, write and iops rates from OSS machines and iops from MDS machines. Multiple filesystems on each OSS or MDS are supported.

lustreHarvest is a simple python script that acts as both data gatherer on the Lustre servers and as aggregator on the machine that spoofs the processed data into ganglia.

![alt text](http://sf.anu.edu.au/~rjh900/git/lustreHarvest/cluster_ops.png "whole cluster iops")
![alt text](http://sf.anu.edu.au/~rjh900/git/lustreHarvest/node_io.png "i/o from a node")

How it Works
------------

data gathering occurs on Lustre OSS/MDS server nodes eg.

   ./lustreHarvest.py host home short

where ''host'' is the name of the machine to send data to, and ''home'' and ''short'' are the names of Lustre OSTs present.

the master aggregator process runs on ''host'' which is typically a management server eg.

   ./lustreHarvest.py

this turns the data it recieves into rates for each client and spoofs these into [ganglia](https://github.com/ganglia/) using the [gmetric.py](https://github.com/ganglia/ganglia_contrib/tree/master/gmetric-python) module.

data is tranferred by sending serialised python objects transported over simple TCP connections. client sends are closely synchronised so that the server can tell when a data gathering sweep is finished, sum and generate statistics for each client, and spoof close to coherent data into ganglia. data integrity (but not authenticity) is verified by md5 sums of the objects.

lustreHarvest transparently handles client and server process disconnections and restarts.
