import os
import os.path
import socket
import subprocess
import sys

if  not os.environ.has_key("GEMFIRE"):
	sys.exit("GEMFIRE environment variable must be configured")
	
GEMFIRE=os.environ["GEMFIRE"]


CLUSTER_HOME=os.path.dirname(sys.argv[0])
CACHE_XML_FILE=os.path.join(CLUSTER_HOME,"config","cache.xml")
SERVER_CLASSPATH=os.path.join(CLUSTER_HOME,"..","target","*")

def locatorport(cnum):
	return 10000 * cnum			

def serverport(cnum, snum):
	return (10000 * cnum) + (100 * snum)
	
def jmxmanagerport(cnum):
	return (10000 * cnum) + 1099
	
def gwayreceiverstartport(cnum):
	return (10000 * cnum) + 2000

def gwayreceiverendport(cnum):
		return (10000 * cnum) + 2999

def httpport(cnum):
		return (10000 * cnum) + 7070


class WanConfig:
	def __init__(self, rl, rdsid):
		self.REMOTE_LOCATORS=rl
		self.REMOTE_DISTRIBUTED_SYSTEM_ID=rdsid


WAN_CONFIG=[None, 
	WanConfig("localhost[{0}]".format(locatorport(2)),"2"),
	WanConfig("localhost[{0}]".format(locatorport(1)),"1")]


LOCATOR_PID_FILE="cf.gf.locator.pid"
SERVER_PID_FILE="cf.gf.server.pid"

def clusterDir(cnum):
	return  CLUSTER_HOME + "/{0}".format(cnum)
	
def locatorDir(cnum):
	return clusterDir(cnum) + "/locator"
	
def serverDir(cnum, snum):
	return clusterDir(cnum) + "/server_{0}".format(snum)

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
		
def startLocator(cnum):
	ensureDirectories(cnum, 0)

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
	

			
