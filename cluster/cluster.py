#!/usr/bin/python

from __future__ import print_function
import clusterdef
import os
import os.path
import socket
import subprocess
import sys


LOCATOR_PID_FILE="cf.gf.locator.pid"
SERVER_PID_FILE="cf.gf.server.pid"

clusterDef = None

def ensureDir(dname):
	if not os.path.isdir(dname):
		os.mkdir(dname)

	
def ensureDirectories(cnum, nodecount):
	ensureDir(CLUSTER_HOME)
	ensureDir(clusterDir(cnum))
	ensureDir(locatorDir(cnum))
	if nodecount > 0:
		for i in range(1,nodecount + 1):
			ensureDir(serverDir(cnum,i))


def pidIsAlive(pidfile):
	if not os.path.exists(pidfile):
		return False
		
	with fopen(pidfile,"r") as f:
		pid = int(f.read())

	psrc = subprocess.call(["ps",str(pid)])
	if psrc == 0:
		return True
	else:
		return False
	

def serverIsRunning(cnum, snum):
	try:
		sock = socket.create_connection(("localhost", serverport(cnum,snum)))
		sock.close()
		return True
	except Exception as x:
		pass
		# ok - probably not running
		
	# now check the pid file
	pidfile = serverDir(cnum,snum) + "/SERVER_PID_FILE"
	return pidIsAlive(pidfile)	
	
def locatorIsRunning(cnum):
	try:
		sock = socket.create_connection(("localhost", locatorport(cnum)))
		sock.close()
		return True
	except Exception as x:
		pass
		# ok - probably not running
		
	# now check the pid file
	pidfile = locatorDir(cnum) + "/LOCATOR_PID_FILE"
	return pidIsAlive(pidfile)	
		
def stopLocator(cnum):
	if not locatorIsRunning(cnum):
		return
		
	subprocess.check_call([GEMFIRE + "/bin/gfsh"
		, "stop", "locator"
		,"--dir=" + locatorDir(cnum)])
		
def startLocator(processName):
	ensureDir()

	if locatorIsRunning(cnum):
		return
		
	subprocess.check_call([GEMFIRE + "/bin/gfsh"
		, "start", "locator"
		,"--dir=" + locatorDir(cnum)
		,"--port={0}".format(locatorport(cnum))
		,"--name=locator" 
		,"--mcast-port=0"
		,"--J=-Dgemfire.distributed-system-id={0}".format(cnum)
		,"--J=-Dgemfire.remote-locators={0}".format(WAN_CONFIG[cnum].REMOTE_LOCATORS)
		,"--J=-DREMOTE_DISTRIBUTED_SYSTEM_ID={0}".format(WAN_CONFIG[cnum].REMOTE_DISTRIBUTED_SYSTEM_ID)
		,"--J=-Dgemfire.jmx-manager-port={0}".format(jmxmanagerport(cnum))
		,"--J=-Dgemfire.http-service-port={0}".format(httpport(cnum))
		])

	
def startCluster(cnum, nodecount):
	ensureDirectories(cnum, nodecount)
	startLocator(cnum)
	processList = []
	dirList = []
	for i in range(1,nodecount + 1):
		if not serverIsRunning(cnum,i):
			proc = subprocess.Popen([GEMFIRE + "/bin/gfsh"
					, "start", "server"
					,"--dir=" + serverDir(cnum, i)
					,"--server-port={0}".format(serverport(cnum,i))
					,"--locators=localhost[{0}]".format(locatorport(cnum))
					,"--classpath={0}".format(SERVER_CLASSPATH)
 					,"--cache-xml-file={0}".format(CACHE_XML_FILE)
					,"--name=server_{0}".format(i) 
					,"--mcast-port=0"
					,"--J=-Dgemfire.distributed-system-id={0}".format(cnum)
					,"--J=-Dgemfire.remote-locators={0}".format(WAN_CONFIG[cnum].REMOTE_LOCATORS)
					,"--J=-DREMOTE_DISTRIBUTED_SYSTEM_ID={0}".format(WAN_CONFIG[cnum].REMOTE_DISTRIBUTED_SYSTEM_ID)
					,"--J=-DGATEWAY_RECEIVER_START_PORT={0}".format(gwayreceiverstartport(cnum))
					,"--J=-DGATEWAY_RECEIVER_END_PORT={0}".format(gwayreceiverendport(cnum))
					])

			processList.append(proc)
			dirList.append(serverDir(cnum, i))
		
	for j in range(0, len(processList)):
		if processList[j].wait() != 0:
			raise Exception("cache server process failed to start - see the logs in {0}".format(dirList[j]))
			
def stopCluster(cnum):
	if not locatorIsRunning(cnum):
		return
		
	rc = subprocess.call([GEMFIRE + "/bin/gfsh"
		, "-e", "connect --locator=localhost[{0}]".format(locatorport(cnum))
		,"-e", "shutdown"])

	# it appears that the return code in this case is not correct
	# will just hope for the best right now	
	
	stopLocator(cnum)
	
def printUsage():
	print('Usage:')
	print('   cluster.py <path-to-cluster-def> start <process-name>')
	print('   cluster.py <path-to-cluster-def> stop <process-name>')
	print('   cluster.py <path-to-cluster-def> status <process-name>')
	print()
	print('Notes:')
	print('* all commands are idempotent')
	

if __name__ == '__main__':
	if len(sys.argv) == 1:
		printUsage()
		sys.exit(0)
		
	clusterDefFile = sys.argv[1]
	if not os.path.isfile(clusterDefFile):
		sys.exit('could not find cluster definition file: ' + clusterDefFile)
		
	with open(clusterDefFile,'r') as f:
		clusterDef = ClusterDef(json.load(f))
		
	if len(sys.argv) < 4:
		sys.exit('invalid input, please provide a command and an object')
		
	cmd = sys.argv[2]
	obj = sys.argv[3]
	
	if clusterDef.isLocatorOnThisHost(obj):
		if cmd == 'start':
			pass
		elif cmd == 'stop':
			pass
		elif cmd == 'status':
			pass
		else:
			sys.exit(cmd + ' is an unkown operation for locators')
	else:
		sys.exit(obj + ' is not defined for this host or is not a known process type')		
	

		