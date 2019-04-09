#!/bin/bash
NUM=$1
#dd if=/dev/zero of=/home/condor/alluxio-data/512file/$NUM-merged.bam bs=8M count=64
#dd if=/dev/zero of=/home/condor/alluxio-data/1Gfile/$NUM-merged.bam bs=8M count=128
#dd if=/dev/zero of=/home/condor/alluxio-data/2Gfile/$NUM-merged.bam bs=8M count=256
#dd if=/dev/zero of=/home/condor/alluxio-data/4Gfile/$NUM-merged.bam bs=8M count=512
dd if=/dev/zero of=/home/condor/alluxio-data/8Gfile/$NUM-merged.bam bs=1M count=1024
alluxio fs stat /8Gfile/$NUM-merged.bam
echo 3 > /proc/sys/vm/drop_caches
