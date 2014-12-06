#!/bin/bash


kill -9 `ps uax | grep "t4tc-stats-monitor.py" | awk '{print $2}'`
python t4tc-stats-monitor.py
