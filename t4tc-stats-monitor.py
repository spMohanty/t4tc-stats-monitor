#!/usr/bin/env python
import glob
import redis
import time
from daemonize import Daemonize
import string
import random
import logging

import threading
import json


def getEntropy():
	return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(16))


redis_server = "t4tc-mcplots-db.cern.ch"
state_folder = "/nfs/shared/mcplots/state/"

debug_mode = False

every_t_seconds = 60*60 ## 1 hour

######Data Types
##Volunteer
volunteers_flush_TTL = 12 * every_t_seconds  #flush_TTL is the timegap in seconds. we can flush all the data before current_time - flush_TTL

pending_flush_TTL = 12 * every_t_seconds  #flush_TTL is the timegap in seconds. we can flush all the data before current_time - flush_TTL

monitor_machines_TTL = 12 * every_t_seconds ## 12 * every_t_seconds seconds

other_TTL = 12 * every_t_seconds

real_time_update_delay = 58

def pending_update():
	while 1:
		if debug_mode: print "In pending_update\n"
		##pending
		r = redis.Redis(host=redis_server, port=6379, db=0)
		p=r.pipeline()

		p.hget("T4TC_MONITOR/TOTAL/", "pending_instant");
		p.hget("T4TC_MONITOR/TOTAL/", "volunteers_instant");
		p.hget("T4TC_MONITOR/TOTAL/", "monitor-machines_instant");
		p.hget("T4TC_MONITOR/TOTAL/", "monitor-load_instant");
		p.hget("T4TC_MONITOR/TOTAL/", "monitor-alerts_instant");
		p.hget("T4TC_MONITOR/TOTAL/", "jobs_completed")
		p.hget("T4TC_MONITOR/TOTAL/", "jobs_failed")

		results = p.execute()
		if debug_mode: print results

		if len(results) == 7:
			for k in range(len(results)):
				if not results[k]: ##Change all None values to 0
					results[k] = 0

			try:		
				timeseries_data_push("T4TC_MONITOR/TOTAL/pending/HIST", "pending", int(str(results[0]).strip()))
				timeseries_data_push("T4TC_MONITOR/TOTAL/online_users/HIST", "volunteers", int(str(results[1]).strip()))
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-machines/HIST", "monitor-machines", int(str(results[2]).strip()))
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-load/HIST", "monitor-load", float(str(results[3]).strip()))
				timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-alerts/HIST", "monitor-alerts", int(str(results[4]).strip()))
				timeseries_data_push("T4TC_MONITOR/TOTAL/jobs_completed/HIST", "jobs_completed" ,int(str(results[5]).strip())-int(str(results[6]).strip())) ##jobs_completed holds the total jobs succeded + failed
				timeseries_data_push("T4TC_MONITOR/TOTAL/jobs_failed/HIST", "jobs_failed" ,int(str(results[6]).strip()))

				##Add overall timeseries logging without expiry
				p = r.pipeline()
				p.hset("T4TC_MONITOR/TOTAL/OVERALL_TIMESERIES", str(time.time()), str(json.dumps(results)))
				p.execute()

			except Exception as inst:
				if debug_mode: print type(inst)     # the exception instance
				if debug_mode: print inst.args

		time.sleep(every_t_seconds)##Repeat this every t seconds


def realtime_update():
	if debug_mode: print "In realtime_update"

	while 1:
			r = redis.Redis(host=redis_server, port=6379, db=0)
			p=r.pipeline()
			if debug_mode: print "="*80
			if debug_mode: print "In realtime_update"

			error = False
			queue_length = 0
			for jm in glob.glob(state_folder+"jm_t4tc-copilot-jm-*"):
				f = open(jm)
				j = f.read()
				try:
					queue_length += int(j)

				except:
					error = True
					if debug_mode: print "Error in queue length ", queue_length," in file :: ", jm
					pass ##Silently pass in case of any errors in the jm file
				f.close()


			if not error:
				if debug_mode: print "queue_length", queue_length			
				# timeseries_data_push("T4TC_MONITOR/TOTAL/pending/HIST", "pending", queue_length)
				p.hset("T4TC_MONITOR/TOTAL/", "pending_instant", queue_length);


			#Get Volunteers
			f = open(state_folder+"volunteers")
			lines = f.readlines()
			if len(lines)>0:
				last_line = lines[-1]
				try:
					# timeseries_data_push("T4TC_MONITOR/TOTAL/online_users/HIST", "volunteers", int(last_line))
					if debug_mode: print "Volunteers :: ",int(last_line)
					if not last_line:
						last_line = 0

					p.hset("T4TC_MONITOR/TOTAL/", "volunteers_instant", int(last_line));
				except Exception as inst:
					if debug_mode: print type(inst)     # the exception instance
					if debug_mode: print inst.args      # arguments stored in .args
					if debug_mode: print inst           # __str__ allows args to be if debug_mode: printed directly
					x, y = inst.args
					if debug_mode: print 'x =', x
					if debug_mode: print 'y =', y
					foo=1

			#Get monitor-machines
			f = open(state_folder+"monitor-machines")
			lines = f.readlines()
			if len(lines)>0:
				last_line = lines[-1]
				try:
					# timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-machines/HIST", "monitor-machines", int(last_line))
					if debug_mode: print "Monitor-Machines ::", int(last_line)
					if not last_line:
						last_line = 0
					p.hset("T4TC_MONITOR/TOTAL/", "monitor-machines_instant", int(last_line));
				except Exception as inst:
					if debug_mode: print type(inst)     # the exception instance
					if debug_mode: print inst.args      # arguments stored in .args
					if debug_mode: print inst           # __str__ allows args to be if debug_mode: printed directly
					x, y = inst.args
					if debug_mode: print 'x =', x
					if debug_mode: print 'y =', y
					foo=1	

			#Get monitor-load
			f = open(state_folder+"monitor-load")
			lines = f.readlines()
			if len(lines)>0:
				last_line = lines[-1]
				try:
					# timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-load/HIST", "monitor-load", float(last_line))
					if debug_mode: print "Monitor-load ::", float(last_line)
					if not last_line:
						last_line = 0

					p.hset("T4TC_MONITOR/TOTAL/", "monitor-load_instant", float(last_line));

				except Exception as inst:
					if debug_mode: print type(inst)     # the exception instance
					if debug_mode: print inst.args      # arguments stored in .args
					if debug_mode: print inst           # __str__ allows args to be if debug_mode: printed directly
					x, y = inst.args
					if debug_mode: print 'x =', x
					if debug_mode: print 'y =', y
					foo=1								



			#Get monitor-load
			f = open(state_folder+"monitor-alerts")
			lines = f.readlines()
			if len(lines)>0:
				last_line = lines[-1]
				try:
					# timeseries_data_push("T4TC_MONITOR/TOTAL/monitor-alerts/HIST", "monitor-alerts", int(last_line))
					if debug_mode: print "Monitor-alerts ::", int(last_line)
					p.hset("T4TC_MONITOR/TOTAL/", "monitor-alerts_instant", int(last_line));

				except Exception as inst:
					if debug_mode: print type(inst)     # the exception instance
					if debug_mode: print inst.args      # arguments stored in .args
					if debug_mode: print inst           # __str__ allows args to be if debug_mode: printed directly
					x, y = inst.args
					if debug_mode: print 'x =', x
					if debug_mode: print 'y =', y
					foo=1	

			# #Get jobs completed and failed
			# r = redis.Redis(host=redis_server, port=6379, db=0)
			# p=r.pipeline()

			results = p.execute()
			if debug_mode: print results

			time.sleep(real_time_update_delay); ## Updated every t seconds


def main():

	pupdate = threading.Thread(target=pending_update)
	rupdate = threading.Thread(target=realtime_update)

	pupdate.start()
	rupdate.start()


	



def timeseries_data_push(key, typ, data): #type == data type as mentioned in the block above
	if debug_mode: print key, typ,data
	r = redis.Redis(host=redis_server, port=6379, db=0)
	p=r.pipeline()

	## Push the data into the sorted set at key with timeestamp as score
	#if debug_mode: print key, int(time.time()), data
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

	#if debug_mode: print int(time.time())-flush_TTL
	p.zremrangebyscore(key, 0, int(time.time())-flush_TTL)
	p.execute()

if debug_mode:
	main()
else:
	daemon = Daemonize(app="t4tc-stats-monitor", pid="/tmp/t4ts-stats-monitor", action=main)
	daemon.start()

