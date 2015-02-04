#!/usr/bin/python

import sys
import wancluster

#expects an argument specifiying the cluster number
if (len(sys.argv) != 3):
	sys.exit("script must be called with exactly two parameter specifying the cluster number and number of servers")

cnum = int(sys.argv[1])
snum = int(sys.argv[2])

wancluster.startCluster(cnum, snum)