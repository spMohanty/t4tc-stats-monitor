#!/usr/bin/env python
import glob
import redis
import time
from daemonize import Daemonize


redis_server = "t4tc-mcplots-db.cern.ch"
state_folder = "/nfs/shared/mcplots/state/"

def main():
	while 1:
		##Get number od jobs
		r = redis.Redis(host=redis_server, port=6379, db=0)
		p = r.pipeline()

		queue_length = 0
		for jm in glob.glob(state_folder+"jm_t4tc-copilot-jm-*"):
			f = open(jm)
			j = f.read()
			try:
				queue_length += int(j)
			except:
				pass ##Silently pass in case of any errors in the jm file	
			f.close()
		# Push queue length to redisi
		p.set("T4TC_MONITOR/TOTAL/pending", queue_length)
		
		#Get Volunteers
		f = open(state_folder+"volunteers")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				p.set("T4TC_MONITOR/TOTAL/online_users", int(last_line))
			except:
				pass # Silently pass in case of errors in the file
		p.execute()
		time.sleep(30);	
	
def timeseries_data_push(key, flush_TTL, data):##flush_TTL is the timegap in miliseconds. we can flush all the data before current_time - flush_TTL
	r = redis.Redis(host=redis_server, port=6379, db=0)
	p=r.pipeline()

	## Push the data into the sorted set at key with timeestamp as score
	p.zadd(key,int(time.time()), data)
	
	## Flush all data before flush_TTL miliseconds
	p.zremrangebyscore(key, 0, int(time.time())-flush_TTL)
	p.execute() 	

daemon = Daemonize(app="t4tc-stats-monitor", pid="/tmp/t4ts-stats-monitor", action=main)
daemon.start()
#main()
#timeseries()


