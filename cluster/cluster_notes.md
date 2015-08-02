The minimal cluster config looks like this:

automatic translation of bind addresses:  if a bind address does not have
a '.' in the name, it is interpreted as a network interface and decoded
to an ip address via the netifaces python package

required cluster level properties
    cluster-dir
    
required properties for locators
    port
    bind-address
    memory
    
required properties for data nodes


every process has to have a unique name

progression
start/stop/status locators
start/stop/status datanodes
start/stop/status cluster
add ability to configure arbitrary placeholders (for disk stores)
add support for classpath
add security
add gateways, ds id
document

```
{
    "global-properties":{
        "gemfire-path": "/Users/rmay/Pivotal/software/Pivotal_GemFire_810_b50625_Linux",
        "java-path" : "/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home",
        "locators" : "localhost[10000]",
        "cluster-home" : "/Users/rmay/Pivotal/gemfire-toolkit/cluster/1"
    },
   "locator-properties" : {
        "locator-port" : 10000,
        "jmx-manager-port" : 11099,
        "http-service-port" : 17070,
        "locator-network-interface" : "en0"
    },
   "datanode-properties" : {
        "server-port" : 1100
    },
    "hosts": { 
        "Randy-May-MacBook-Pro.local" : {  
            "host-properties" :  {
             },
            "processes" :  {  
                "locator" : {
                      "type" : "locator",
                 },
                 "server1" : {
                    "type" : "datanode"
                 },
                 "server2" : {
                    "type" : "datanode"
                 }
            }
        }
   }
}
```

* Arbitrary options