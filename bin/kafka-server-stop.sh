#!/bin/sh
ps ax | egrep -i 'kafka|.Kafka' | grep -v grep | awk '{print $1}' | xargs kill -SIGINT
