#!/usr/bin/python

import os.path
import shutil
import sys
import wancluster

#expects an argument specifiying the cluster number
if (len(sys.argv) != 2):
	sys.exit("script must be called with exactly one parameter specifying the cluster number ")

cnum = int(sys.argv[1])

wancluster.stopCluster(cnum)

dirname = wancluster.clusterDir(cnum)
if os.path.exists(dirname):
	shutil.rmtree(dirname)