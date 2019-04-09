#!/bin/bash
#sleep 10
time(dd if=$1 of=/dev/null bs=8M count=$2) 

#time(dd if=$1 of=myfile bs=8M count=60)

