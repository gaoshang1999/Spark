#!/bin/bash
hadoop fs -mkdir /user/cloudera/part2  /user/cloudera/project/part2/input
echo "B12 C31 D76 A12 B76 B12 D76 C31 A10 B12 D76" > input
echo "C31 D76 B12 A12 C31 D76 B12 A12 D76 A12 D76" >> input
hadoop fs -put input /user/cloudera/part2/input
hadoop fs -rm -r /user/cloudera/part2/output/*
hadoop fs -rmdir -r /user/cloudera/part2/output
hadoop jar wc.jar PairsRelativeFrequency  /user/cloudera/part2/input /user/cloudera/part2/output
