#!/bin/bash

cd /var/www/html/hong_pet/財政部稅籍/
printf "%0.s-" {1..50}
printf "\n"
STARTDATE=`date +"%Y-%m-%d %T"`
echo Start : ${STARTDATE}
/home/ubuntu/miniconda3/bin/python3.8 tax_auto_v3.py
ENDDATE=`date +"%Y-%m-%d %T"`
echo END : ${ENDDATE}
printf "%0.s-" {1..50}
printf "\n"
