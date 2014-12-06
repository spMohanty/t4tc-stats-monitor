#!/usr/bin/env python
import glob
import redis
import time
from daemonize import Daemonize
import string
import random

def getEntropy():
	return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(16))


redis_server = "t4tc-mcplots-db.cern.ch"
state_folder = "/nfs/shared/mcplots/state/"

######Data Types
##Volunteer
volunteers_flush_TTL = 10  #flush_TTL is the timegap in seconds. we can flush all the data before current_time - flush_TTL

pending_flush_TTL = 10  #flush_TTL is the timegap in seconds. we can flush all the data before current_time - flush_TTL

monitor_machines_TTL = 10

def main():
	while 1:
		queue_length = 0
		for jm in glob.glob(state_folder+"jm_t4tc-copilot-jm-*"):
			f = open(jm)
			j = f.read()
			try:
				queue_length += int(j)
			except:
				pass ##Silently pass in case of any errors in the jm file
			f.close()

		timeseries_data_push("T4TC_MONITOR/TOTAL/pending/HIST", "pending", queue_length)

		#Get Volunteers
		f = open(state_folder+"volunteers")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/online_users/HIST", "volunteers", int(last_line))
			except:
				pass # Silently pass in case of errors in the file

		#Get monitor-machines
		f = open(state_folder+"monitor-machines")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-machines/HIST", "monitor-machines", int(last_line))
			except:
				pass # Silently pass in case of errors in the file		

		#Get monitor-load
		f = open(state_folder+"monitor-load")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-load/HIST", "monitor-load", float(last_line))
			except:
				pass # Silently pass in case of errors in the file									



		#Get monitor-load
		f = open(state_folder+"monitor-alerts")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-alerts/HIST", "monitor-alerts", int(last_line))
			except:
				pass # Silently pass in case of errors in the file		

		time.sleep(1); ## Updated every second


def timeseries_data_push(key, typ, data): #type == data type as mentioned in the block above
	#print key, typ,data
	r = redis.Redis(host=redis_server, port=6379, db=0)
	p=r.pipeline()

	## Push the data into the sorted set at key with timeestamp as score
	#print key, int(time.time()), data
	p.zadd(key, getEntropy()+"_"+str(data), int(time.time()))

	## Flush all data before flush_TTL seconds
	flush_TTL = 10**100
	if(typ=="volunteers"):
		flush_TTL = volunteers_flush_TTL
	elif(typ=="pending"):
		flush_TTL = pending_flush_TTL
	elif(typ=="monitor-machines"):
		flush_TTL = monitor_machines_TTL	## User common TTL for monitor-* type data	
	elif(typ=="monitor-load"):
		flush_TTL = monitor_machines_TTL		
	elif(typ=="monitor-alerts"):
		flush_TTL = monitor_machines_TTL		

	#print int(time.time())-flush_TTL
	p.zremrangebyscore(key, 0, int(time.time())-flush_TTL)
	p.execute()

daemon = Daemonize(app="t4tc-stats-monitor", pid="/tmp/t4ts-stats-monitor", action=main)
daemon.start()
# main()
#timeseries()