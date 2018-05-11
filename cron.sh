#! /usr/bin/env bash
 
source /home/ubuntu/.bashrc

cd /www/_v3

ruby cron.rb > cron.log 2>&1


