#Load Test RoadMap#
This feature is a framework for driving load against a GemFire application.
Some of its key attributes are:
* pluggable data generation (via Spring IoC)
* pluggable test scenario (also via Spring IoC)
* uses Spring Data GemFire
* allows configuration of multiple concurrent threads
* configurable "throttle" mechanism drives a fixed load 
* reports response time distribution statistics

#Milestones#
##1. Initial Build##
The load tester application is a maven based project which includes the latest
Spring Data GemFire version as its main dependency. It can be imported into STS
using "File/Import/Maven/Existing Maven Projects".  The configuration files
looks something like the one below.

Key implementation features are:
* all data is generated before test threads are actually started so that data
generation does not affect throughput measurements.
* response times are collected and when the test is stopped, response time
quartiles are displayed.
* The SimpleDataGenerator is just a placeholder. It does the simplest possible
thing, like just returning the same TestEntry object every time, but with a
different key.  Additional data generators will be added later.


```xml
<?xml version="1.0" encoding="UTF-8"?>
<beans xmlns="http://www.springframework.org/schema/beans"
	xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:gfe="http://www.springframework.org/schema/gemfire"
	xmlns:util="http://www.springframework.org/schema/util"
	xsi:schemaLocation="http://www.springframework.org/schema/beans http://www.springframework.org/schema/beans/spring-beans.xsd
		http://www.springframework.org/schema/gemfire http://www.springframework.org/schema/gemfire/spring-gemfire.xsd
		http://www.springframework.org/schema/util http://www.springframework.org/schema/util/spring-util-4.0.xsd">


	<gfe:client-cache id="cache" pdx-read-serialized="false"
		pool-name="pool" pdx-serializer-ref="pdxSerializer" />

	<gfe:pool id="pool" read-timeout="10000">
		<gfe:locator host="wmay-mbp.local" port="9999" />
	</gfe:pool>


	<gfe:client-region id="airportRegion" name="Test" 
		cache-ref="cache" pool-name="pool" shortcut="PROXY" />


	<bean id="pdxSerializer" class="com.gemstone.gemfire.pdx.ReflectionBasedAutoSerializer">
		<constructor-arg>
			<util:list value-type="java.lang.String">
				<value>com.acme.domain.*</value>
			</util:list>
		</constructor-arg>
	</bean>
	
	<bean id="loaderTester" class="io.pivotal.de.gemfire.loadtester.LoadTester">
        <property name="threads" value="2" />
        <property name="executeEveryMs" value="1000" />
        <property name="dataGenerator" ref="simpleEntryDataGenerator" />
	</bean>
    
    <bean id="simpleEntryDataGenerator" class="io.pivotal.de.gemfire.loadtester.SimpleEntryDataGenerator">
    </bean>

</beans>

```

