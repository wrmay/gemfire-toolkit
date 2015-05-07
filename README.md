# Overview

A collection of useful utilities for administering GemFire

These utilities are released as two deployment objects.

The first of these is a “*.tar.gz*” file containing all of the utility scripts and code. This can
be unpacked wherever appropriate, and provided connectivity is not blocked by firewalls, can
connect across the network to a Gemfire cluster.
 
The second is a “*.jar*” file. Some but not all of the utilities also require the extra code in
this file to be present in the Gemfire cluster. Which utilities need this is indicated below.

##checkred 
*(client-side only)*
The “checkred” tool can report regions that are “at risk” because they are not fully redundant. 
The tool only reports on the redundancy status of partitioned regions.  Replicate regions are 
redundant so long as more than one member of the distributed system is running.

##gemtouch
*(client-side & server-side)*
The "gemtouch" tool can be used to facilitate recovery over WAN

This utility connects to the JMX manager to determine the list of all regions in a distributed
system.  It then creates a normal client connection via locators and runs the "Touch" function
on all regions.  The "Touch" function gets and then puts every entry in the region.  This causes 
each region to forwarded all entries over any running senders that they are connected to.

If an entry happens to be updated or removed between the time that this utility performs a get and
the time that it attempts a put, the put will be aborted to avoid accidentally undoing an update 
from an external source.  In other words, the utility is safe to run on an active cluster.

##localExport
*(client-side only)*
This “localExport” tool copies data from a remote Gemfire cluster to the local machine.

Any number of regions may be requested, explicitly named or using a wildcard. One extract
file is produced for each region.

```
localExport.py locatorhost1[port1],locatorhost2[port2] aaa bbb c*
```

Each extract file is as human-readible as can be. The data itself may be binary. The files
produced are named after the region, the timestamp, and the format.

##localImport
*(client-side only)*
This is the reverse of the export, copying files of data from the local machine into a
remote Gemfire cluster.

```
localImport.py locatorhost1[port1],locatorhost2[port2] /tmp/aaa.1431006707006.adp /tmp/bbb.1431006707006.adp
```

There is deliberately no validation on the files being imported. This gives two advantages.
1. The name of the target region is taken from the name part of the file. You can change the
	target region for the import by simply renaming the file.
2. With care, you can edit the file prior to import, and change the data being imported. There
	are no checksums, but you need to avoid perturbing the file format.


##remoteExport
*(client-side & server-side)*
This “remoteExport” tool copies data from a remote Gemfire cluster to the remote machine.

The “localExport” retrieves all data to the local machine, creating a single file with all the
contents of a region. The “remoteExport” asks each server in the cluster to export the data
that only that server has, to a directory on that host.

If there 10 servers in the cluster, this makes 10 extract files for a region with a 1/10<sup>th</sup> of
the data in each.
1. This export would take 1/10<sup>th</sup> of the time to run.
2. There are now 10 files to collate, and they are not on the local machine

```
remoteExport.py locatorhost1[port1],locatorhost2[port2] a b c*
```

The files produced by “remoteExport” and “localExport” are in the same format, so the import
can use either.


##trace / untrace##
*(client-side & server-side)*

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


# Installation
Unpack the tarball: `gemtoolsDist-==VERSION==-client.tar.gz`

This will create a directory, "gemtools" which contains everything needed to run the script (except a JVM)

There is a server side component to this utility, use gfsh to deploy `gemtoolsDist/target/gemtoolsDist-==VERSION==-server.jar` to the cluster that you want to act upon.

Set the JAVA_HOME environment variable to point to a java installation. 
The script will execute the JVM at $JAVA_HOME/bin/java


##checkred usage

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

##gemtouch usage

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

##localExport usage

###Usage

The first argument is connection information. Second and other arguments specify region names.

Connection information is the locator pairing for the target region. This can be copied from the `gemfire.properties` file for the target cluster.

###Examples
`localExport.py locatorhost1[port1],locatorhost2[port2] a b`
or
`localExport.py locatorhost1[port1],locatorhost2[port2] ‘c*’`
or
`localExport.py locatorhost1[port1],locatorhost2[port2] ‘*’`

The first example will export regions with the specific names `a` and `b`.
The second example will export regions whose names begin with `c`.
The last example will export all regions, except for Gemfire system regions which are always excluded.

When using wildcards, be careful for the shell expanding them before passing to the script, which
may not be what you wish.

##localImport usage

###Usage

The first argument is connection information. Second and other arguments specify files to import.

Connection information is as for `localExport`. You do not need to import into the cluster that you exported from.

###Examples
`localImport.py locatorhost1[port1],locatorhost2[port2] /tmp/aaa.1431006707006.adp `
or
`localExport.py locatorhost1[port1],locatorhost2[port2] /tmp/*.1431006707006.adp`

The first example will import the specified file into a region named `aaa`.

The second example uses the shell to find all extract files with the same timestamp, presumably made
by the one export.

##remoteExport usage

###Usage

All arguments to the `remoteExport.py` have the same meaning as for `localExport.py`


#Things That Have Not Been Tested
1. with secure JMX
2. subregions - may or may not fully support subregions

#Known Issues
1. Server groups not supported.
2. The import/export utility only supports dynamically created regions, in this version.
3. Local import can timeout if it takes too long for the servers to respond for which data is available.




