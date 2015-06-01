# Overview

A collection of useful utilities for administering GemFire

##Python cluster control scripts##
A very convenient and flexible set of Python scripts
preconfigured to launch 2 clusters connected via gateway

##gemtouch##
The "gemtouch" tool can be used to facilitate recovery over WAN

This utility connects to the JMX manager to determine the list of all regions in a distributed
system.  It then creates a normal client connection via locators and runs the "Touch" function
on all regions.  The "Touch" function gets and then puts every entry in the region.  This causes 
each region to forwarded all entries over any running senders that they are connected to.

If an entry happens to be updated or removed between the time that this utility performs a get and
the time that it attempts a put, the put will be aborted to avoid accidentally undoing an update 
from an external source.  In other words, the utility is safe to run on an active cluster.

##checkred##
The "checkred" tool can report regions that are "at risk" because they are not fully redundant. 
The tool only reports on the redundancy status of partitioned regions.  Replicate regions are 
redundant so long as more than one member of the distributed system is running.

##trace / untrace##

you must deploy the gemtools jar using the "gfsh deploy" command,
then ...

to install tracing on a region

```
trace.py locatorhost[port] /SomeRegion
```

to remove tracing on a region

```
untrace.py locatorhost[port] /SomeRegion
```


# Installation#
Unpack the tarball: gemtools-VERSION-runtime.tar.gz

This will create a directory, "gemtools" which contains everything needed to run the script (except a JVM)

There is a server side component to this utility, use gfsh to deploy gemtools/lib/gemtools-VERSION.jar to 
the cluster that you want to act upon.

Set the JAVA_HOME environment variable to point to a java installation. 
The script will execute the JVM at $JAVA_HOME/bin/java

#python script usage#

The python control scripts  and all required files are located in the "cluster" directory.
This directory can be copied to any place that is convenient.

The layout of file system inside of the cluster directory is described below:

```
cluster
|--1              #working direcory for cluster 1
|  |--locator     #working directory for locator (logs, stats, disk stores)
|  |--server_1    #working directory for server_1 (logs, stats, disk stores)
|  |--server_2    #working directory for server_2 (logs, stats, disk stores)
|
|--2              #working directory for cluster_2
|  |--locator     #working directory for locator (logs, stats, disk stores)
|  |--server_1    #working directory for server_1 (logs, stats, disk stores)
|  |--server_2    #working directory for server_2 (logs, stats, disk stores)
|
|--config         #shared configuration
|  |--cache.xml
|
|-removecluster.py #script, removes a cluster
|-startcluster.py  #script, starts a cluster
|-stopcluster.py   #script, stops a cluster
|-wancluster.py    #scrip, shared, not directly invoked
```

Note that the "1" and "2" subtrees contain no source or scripts, only working files
such as logs, stats and disk stores.  They can be removed without breaking anything.

####setup procedure####
1. copy the "cluster" directory to a location of your choosing
2. set the GEMFIRE environment variable to the location of the GemFire installation you
will use
3. set the JAVA_HOME environment variable to the location of a JDK
4. if you wish to use specific pre-defined region configurations, put them in "config/cache.xml"
5. all commands should be executed in the cluster home directory

#### usage examples####
_start cluster 1 with 2 servers_
```
./startcluster.py 1 2
```

_start cluster 2 with 3 servers_
```
./startcluster.py 2 3
```

_stop cluster 1_
```
./stopcluster.py 1
```

_remove cluster 1_ (removes all disk stores, logs, stats)
```
./removecluster.py 1
```

####ports####

The clusters will use the following ports

|type                   | cluster 1         | cluster 2         |
|-----------------------|-------------------|-------------------|
| locator               |             10000 |             20000 |
| cache servers         | 10101,10102, ...  | 20101,20102, ...  |
| jmx manager           |             11099 |             21099 |
| gateway receivers     | 12000-12999       | 22000-22999       |
| http (Pulse)          |             17070 |             27070 |


#gemtouch usage#

example: 

gemtouch.py --jmx-manager-host=abc --jmx-manager-port=123 --jmx-username=fred --jmx-password=pass --rate-per-thread=100

	--jmx-manager-host and --jmx-manager-port are requires and must point to a GemFire jmx manager (usually the locator)
	  if you are not sure of the port number try 1099
	  
	--jmxusername and --jmx-manager-password are optional but if either is present then both must be provided
	
	--rate-per-second is optional - acts a a throttle if present

	if the metadata region (__regionAttributesMetadata by default) is  present it will be touched first
	
	the name of the metadata region can be set with the --metadata-region-name option
	
	after touching the metadata region the program will pause for 20s to allow for propagation
	the length of the wait (in seconds)  can be set using the --region-creation-delay option



#checkred usage

basic usage

checkred.py --jmx-manager-host=abc --jmx-manager-port=123 --jmx-username=fred --jmx-password=pass 
	--jmx-manager-host and --jmx-manager-port must point to a GemFire jmx manager (usually the locator)
	--jmxusername and --jmx-manager-password are optional but if either is present the other must also be provided

	checkred will return with a 0 exit code if all partition regions have redundancy
	checkred will return with a 1 exit code if any partition regions do not have redundancy

additional options
	By default, checkredundancy will report only partition regions that do not have redundancy
	--verbose will cause checkredundancy to report redundancy of all regions
	--wait=20 will cause checkredundancy to wait up to 20s for redundancy to be established



#Things That Have Not Been Tested
1. with secure JMX
3. subregions - may or may not fully support subregions

#Known Issues
1. Server groups not supported.




