import socket

class ClusterDef:
    
    def __init__(self, cdef):
        self.clusterDef = cdef
        
    def isProcessOnThisHost(self, processName, processType):
        thisHost = socket.gethostname()
        if thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(thisHost))

        if not processName in self.clusterDef['hosts'][thisHost]['processes']:
            return False
        
        process = self.clusterDef['hosts'][thisHost]['processes'][processName]
        return process['type'] == processType
        
    def isLocatorOnThisHost(self, processName):
        return self.isProcessOnThisHost(self, processName, 'locator')
    
    def isDatanodeOnThisHost(self, processName):
        return self.isProcessOnThisHost(self, processName, 'datanode')
    
            
            
        
    
    