#! /usr/bin/env bash
 
source /home/ubuntu/.bashrc

cd /www/_csv

ruby cron.rb > cron.log 2>&1


