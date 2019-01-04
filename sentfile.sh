#!/bin/bash
REMOTE_DIR=cn17633:/home/condor/alluxio
for i in `find . -name *jar`;do(scp $i $REMOTE_DIR/$i);done
