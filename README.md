# Overview

A collection of useful utilities for administering GemFire

##gemtouch
The "gemtouch" tool can be used to facilitate recovery over WAN

This utility connects to the JMX manager to determine the list of all regions in a distributed
system.  It then creates a normal client connection via locators and runs the "Touch" function
on all regions.  The "Touch" function gets and then puts every entry in the region.  This causes 
each region to forwarded all entries over any running senders that they are connected to.

If an entry happens to be updated or removed between the time that this utility performs a get and
the time that it attempts a put, the put will be aborted to avoid accidentally undoing an update 
from an external source.  In other words, the utility is safe to run on an active cluster.

##checkred
The "checkred" tool can report regions that are "at risk" because they are not fully redundant. 
The tool only reports on the redundancy status of partitioned regions.  Replicate regions are 
redundant so long as more than one member of the distributed system is running.

# Installation
Unpack the tarball: gemtools-VERSION-runtime.tar.gz

This will create a directory, "gemtools" which contains everything needed to run the script (except a JVM)

There is a server side component to this utility, use gfsh to deploy gemtools/lib/gemtools-VERSION.jar to 
the cluster that you want to act upon.

Set the JAVA_HOME environment variable to point to a java installation. 
The script will execute the JVM at $JAVA_HOME/bin/java


#gemtouch usage

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




