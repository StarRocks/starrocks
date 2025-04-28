#!/bin/bash
ps aux|grep app.py|grep -v grep|grep python|awk '{print $2}'|xargs kill -9
