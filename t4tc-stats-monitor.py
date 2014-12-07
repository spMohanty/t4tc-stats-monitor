#!/usr/bin/env python
import glob
import redis
import time
from daemonize import Daemonize
import string
import random
import logging

import threading


def getEntropy():
	return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(16))


redis_server = "t4tc-mcplots-db.cern.ch"
state_folder = "/nfs/shared/mcplots/state/"

every_t_seconds = 1*60*10 ## 10minutes

######Data Types
##Volunteer
volunteers_flush_TTL = 12 * every_t_seconds  #flush_TTL is the timegap in seconds. we can flush all the data before current_time - flush_TTL

pending_flush_TTL = 12 * every_t_seconds  #flush_TTL is the timegap in seconds. we can flush all the data before current_time - flush_TTL

monitor_machines_TTL = 12 * every_t_seconds ## 12 * every_t_seconds seconds

other_TTL = 12 * every_t_seconds


def pending_update():
	while 1:
		error = False
		queue_length = 0
		for jm in glob.glob(state_folder+"jm_t4tc-copilot-jm-*"):
			f = open(jm)
			j = f.read()
			try:
				queue_length += int(j)

			except:
				error = True
				print "Error in queue length ", queue_length," in file :: ", jm
				pass ##Silently pass in case of any errors in the jm file
			f.close()


		if not error:
			## Update Queue Length in database
			r = redis.Redis(host=redis_server, port=6379, db=0)
			p=r.pipeline()
			p.hset("T4TC_MONITOR/TOTAL/", "pending_instant", queue_length)
			p.execute()

		time.sleep(2)##Repeat this every two seconds


def main():

	t = threading.Thread(target=pending_update)
	t.start()

	while 1:
		print "="*80
		error = False
		queue_length = 0
		for jm in glob.glob(state_folder+"jm_t4tc-copilot-jm-*"):
			f = open(jm)
			j = f.read()
			try:
				queue_length += int(j)

			except:
				error = True
				print "Error in queue length ", queue_length," in file :: ", jm
				pass ##Silently pass in case of any errors in the jm file
			f.close()


		if not error:
			print "queue_length", queue_length			
			timeseries_data_push("T4TC_MONITOR/TOTAL/pending/HIST", "pending", queue_length)

		#Get Volunteers
		f = open(state_folder+"volunteers")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/online_users/HIST", "volunteers", int(last_line))
			except Exception as inst:
				print type(inst)     # the exception instance
				print inst.args      # arguments stored in .args
				print inst           # __str__ allows args to be printed directly
				x, y = inst.args
				print 'x =', x
				print 'y =', y
				foo=1

		#Get monitor-machines
		f = open(state_folder+"monitor-machines")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-machines/HIST", "monitor-machines", int(last_line))
			except Exception as inst:
				print type(inst)     # the exception instance
				print inst.args      # arguments stored in .args
				print inst           # __str__ allows args to be printed directly
				x, y = inst.args
				print 'x =', x
				print 'y =', y
				foo=1	

		#Get monitor-load
		f = open(state_folder+"monitor-load")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-load/HIST", "monitor-load", float(last_line))
			except Exception as inst:
				print type(inst)     # the exception instance
				print inst.args      # arguments stored in .args
				print inst           # __str__ allows args to be printed directly
				x, y = inst.args
				print 'x =', x
				print 'y =', y
				foo=1								



		#Get monitor-load
		f = open(state_folder+"monitor-alerts")
		lines = f.readlines()
		if len(lines)>0:
			last_line = lines[-1]
			try:
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-alerts/HIST", "monitor-alerts", int(last_line))
			except Exception as inst:
				print type(inst)     # the exception instance
				print inst.args      # arguments stored in .args
				print inst           # __str__ allows args to be printed directly
				x, y = inst.args
				print 'x =', x
				print 'y =', y
				foo=1	

		#Get jobs completed and failed
		r = redis.Redis(host=redis_server, port=6379, db=0)
		p=r.pipeline()

		p.hget("T4TC_MONITOR/TOTAL/", "jobs_completed")
		p.hget("T4TC_MONITOR/TOTAL/", "jobs_failed")

		result = p.execute()
		if len(result)==2:
			timeseries_data_push("T4TC_MONITOR/TOTAL/jobs_completed/HIST", "jobs_completed" ,int(result[0])-int(result[1])) ##jobs_completed holds the total jobs succeded + failed
			timeseries_data_push("T4TC_MONITOR/TOTAL/jobs_failed/HIST", "jobs_failed" ,int(result[1]))


		time.sleep(every_t_seconds); ## Updated every t seconds




def timeseries_data_push(key, typ, data): #type == data type as mentioned in the block above
	print key, typ,data
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
	else:
		flush_TTL = other_TTL

	#print int(time.time())-flush_TTL
	p.zremrangebyscore(key, 0, int(time.time())-flush_TTL)
	p.execute()


daemon = Daemonize(app="t4tc-stats-monitor", pid="/tmp/t4ts-stats-monitor", action=main)
daemon.start()
# main()

