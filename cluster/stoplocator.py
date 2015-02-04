#!/usr/bin/python

import subprocess
import sys
import wancluster

#expects an argument specifiying the locator number
if (len(sys.argv) != 2):
	sys.exit("script must be called with exactly one parameter specifying the cluster numnber")

cnum = int(sys.argv[1])

wancluster.stopLocator(cnum)