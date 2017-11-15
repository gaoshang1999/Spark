#!/bin/bash
hadoop fs -rm -r /user/cloudera/part4
hadoop fs -mkdir /user/cloudera/part4  /user/cloudera/part4/input
echo "B12 C31 D76 A12 B76 B12 D76 C31 A10 B12 D76" > input
echo "C31 D76 B12 A12 C31 D76 B12 A12 D76 A12 D76" >> input
hadoop fs -put input /user/cloudera/part4/input
hadoop fs -rm -r /user/cloudera/part4/output
hadoop jar wc.jar part4.HybridRelativeFrequency  /user/cloudera/part4/input /user/cloudera/part4/output
