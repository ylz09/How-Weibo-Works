#!/bin/bash -

timestamp=`date +%s`
cmd="./hbase_weibo_export.sh \
--start_time=\"2015-08-04 11\" \
--end_time=\"2015-08-04 11\" \
--output_path=/tmp/yangming5/hadoop_testing/$timestamp \
--task_name=yangming5_tools-testing \
--task_queue=production \
--compress=FALSE \
--filter_accept=QI#UID#CONTENT#FWNUM \
--match_expression=CONTENT,CO,王源#QI,EQ,32 \
--doc_action=M"

echo "${cmd}"
eval $cmd

