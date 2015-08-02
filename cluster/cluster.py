#!/usr/bin/python

from __future__ import print_function
import clusterdef
import json
import netifaces
import os
import os.path
import socket
import subprocess
import sys


LOCATOR_PID_FILE="cf.gf.locator.pid"
SERVER_PID_FILE="vf.gf.server.pid"

clusterDef = None


def ensureDir(dname):
	if not os.path.isdir(dname):
		os.mkdir(dname)
		
def locatorDir(processName):
	clusterHomeDir = clusterDef.locatorProperty(processName, 'cluster-home')
	return(os.path.join(clusterHomeDir,processName))

def datanodeDir(processName):
	clusterHomeDir = clusterDef.datanodeProperty(processName, 'cluster-home')
	return(os.path.join(clusterHomeDir,processName))


def pidIsAlive(pidfile):
	if not os.path.exists(pidfile):
		return False
		
	with open(pidfile,"r") as f:
		pid = int(f.read())

	proc = subprocess.Popen(["ps",str(pid)], stdin=subprocess.PIPE,
		stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
	
	proc.communicate()
	
	if proc.returncode == 0:
		return True
	else:
		return False
	
def serverIsRunning(processName):
	try:
		port = clusterDef.locatorProperty(processName, 'server-port')
		bindAddress = clusterDef.translateBindAddress(clusterDef.datanodeProperty(processName, 'server-bind-address'))
		
		sock = socket.create_connection(bindAddress, port)
		sock.close()
		
		return True
	except Exception as x:
		pass
		# ok - probably not running
		
	# now check the pid file
	pidfile = os.path.join(clusterDef.datanodeProperty(processName, 'cluster-home'), processName, SERVER_PID_FILE)
	result = pidIsAlive(pidfile)	
	return result
	
def locatorIsRunning(processName):
	port = clusterDef.locatorProperty(processName, 'port')
	bindAddress = clusterDef.translateBindAddress(clusterDef.locatorProperty(processName, 'bind-address'))
	try:
		sock = socket.create_connection(bindAddress, port)
		sock.close()
		return True
	except Exception as x:
		pass
		# ok - probably not running
		
	# now check the pid file
	pidfile = os.path.join(clusterDef.locatorProperty(processName, 'cluster-home'), processName, LOCATOR_PID_FILE)
	return pidIsAlive(pidfile)	
		
def stopLocator(processName):
	GEMFIRE = clusterDef.locatorProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.locatorProperty(processName,'java-home')
	
	if not locatorIsRunning(processName):
		print('{0} is not running'.format(processName))
		return
	try:	
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "stop", "locator"
			,"--dir=" + locatorDir(processName)])
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)

def stopServer(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.datanodeProperty(processName,'java-home')
	
	if not serverIsRunning(processName):
		print('{0} is not running'.format(processName))
		return
	try:	
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "stop", "server"
			,"--dir=" + datanodeDir(processName)])
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)


def statusLocator(processName):
	GEMFIRE = clusterDef.locatorProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.locatorProperty(processName,'java-home')
	
	try:
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "status", "locator"
			,"--dir=" + locatorDir(processName)])
		
	except subprocess.CalledProcessError as x:
		sys.exit(x.output)

def statusServer(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.datanodeProperty(processName,'java-home')
	
	try:
		subprocess.check_call([os.path.join(GEMFIRE,'bin','gfsh')
			, "status", "server"
			,"--dir=" + datanodeDir(processName)])
		
	except subprocess.CalledProcessError as x:
		sys.exit(x.output)

		
def startLocator(processName):
	GEMFIRE = clusterDef.locatorProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.locatorProperty(processName,'java-home')
	
	ensureDir(clusterDef.locatorProperty(processName, 'cluster-home'))
	ensureDir(locatorDir(processName))

	if locatorIsRunning(processName):
		print('locator {0} is already running'.format(processName))
		return
	
	cmdLine = [os.path.join(GEMFIRE,'bin','gfsh')
		, "start", "locator"
		,"--dir=" + locatorDir(processName)
		,"--port={0}".format(clusterDef.locatorProperty(processName, 'port'))
		,'--bind-address={0}'.format(clusterDef.locatorProperty(processName,'bind-address'))
		,"--name={0}".format(processName)]
	
	cmdLine[len(cmdLine):] = clusterDef.gfshArgs('locator',processName)
	
	try:
		subprocess.check_call(cmdLine)
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)


def startServer(processName):
	GEMFIRE = clusterDef.datanodeProperty(processName,'gemfire')
	os.environ['GEMFIRE'] = GEMFIRE
	os.environ['JAVA_HOME'] = clusterDef.datanodeProperty(processName,'java-home')
	
	ensureDir(clusterDef.datanodeProperty(processName, 'cluster-home'))
	ensureDir(datanodeDir(processName))
	
	if serverIsRunning(processName):
		print('{0} is already running'.format(processName))
		return
	
	cmdLine = [os.path.join(GEMFIRE,'bin','gfsh')
		, "start", "server"
		,"--dir=" + datanodeDir(processName)
		,"--name={0}".format(processName)
		,"--server-bind-address={0}".format(clusterDef.datanodeProperty(processName,'server-bind-address'))
		,"--server-port={0}".format(clusterDef.datanodeProperty(processName,'server-port'))
		]
	
	cmdLine[len(cmdLine):] = clusterDef.gfshArgs('datanode',processName)
	
	try:
		subprocess.check_call(cmdLine)
	except subprocess.CalledProcessError as x:
		sys.exit(x.message)

	
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
		clusterDef = clusterdef.ClusterDef(json.load(f))
		
	if len(sys.argv) < 4:
		sys.exit('invalid input, please provide a command and an object')
		
	cmd = sys.argv[2]
	obj = sys.argv[3]
	
	if clusterDef.isLocator(obj):
		if cmd == 'start':
			startLocator(obj)
		elif cmd == 'stop':
			stopLocator(obj)
		elif cmd == 'status':
			statusLocator(obj)
		else:
			sys.exit(cmd + ' is an unkown operation for locators')
			
	elif clusterDef.isDatanode(obj):
		if cmd == 'start':
			startServer(obj)
		elif cmd == 'stop':
			stopServer(obj)
		elif cmd == 'status':
			statusServer(obj)
		else:
			sys.exit(cmd + ' is an unkown operation for datanodes')
		
		
	else:
		sys.exit(obj + ' is not defined for this host or is not a known process type')		
	

		