import socket

class ClusterDef:
    
    def __init__(self, cdef):
        self.clusterDef = cdef
        self.thisHost = socket.gethostname()
        
    #TODO - maybe it would make more sense for all methods to
    # target "this host" implicitly
    def isProcessOnThisHost(self, processName, processType):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))

        if not processName in self.clusterDef['hosts'][self.thisHost]['processes']:
            return False
        
        process = self.clusterDef['hosts'][self.thisHost]['processes'][processName]
        return process['type'] == processType
        
    def isLocator(self, processName):
        return self.isProcessOnThisHost(self, processName, 'locator')
    
    def isDatanode(self, processName):
        return self.isProcessOnThisHost(self, processName, 'datanode')

    # raises an exception if a process with the given name is not defined for
    # this host
    def processProps(self, processName):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))

        if not processName in self.clusterDef['hosts'][self.thisHost]['processes']:
            raise Exception('{0} is not a valid process name on this host ({1})'.format(processName,self.thisHost))
        
        return self.clusterDef['hosts'][self.thisHost]['processes'][processName]
    
    #host props are optional - if they are not defined in the file an empty
    #dictionary will be returned
    def hostProps(self):
        if self.thisHost not in self.clusterDef['hosts']:
            raise Exception('this host ({0}) not found in cluster definition'.format(self.thisHost))
        
        if 'host-properties' in self.clusterDef['hosts'][self.thisHost]:
            return self.clusterDef['hosts'][self.thisHost]['host-properties']
        else:
            return dict()
    
    def locatorProperty(self, processName, propertyName):
        pProps = self.processProps(processName)
        if propertyName in pdef:
            return pdef[propertyName]
        
        hostProps = self.hostProps()
        if propertyName in hostProps:
            return hostProps[propertyName]
        
        # now check locator props
        # now check global props
        
            
        
        
        
    
    