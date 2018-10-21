#!/bin/bash
# Shanghai

function CHECK() {
  export HADOOP_HOME=/opt/hadoop/program
  hadoop_bin=$HADOOP_HOME/bin-mapreduce1/hadoop
  check_dir=`$hadoop_bin fs -ls $2`
  date_now=`date +"%Y-%m-%d %H:%M:%S"`
  echo "$date_now:: file:"$1", check dir: "$2 >> get_source.log 
  if [ ${#check_dir} -lt 1 ]; then
    echo "$date_now:: file:"$1", check dir: "$2" not exist, so return 0" >> get_source.log 
    echo 0
    return 0
  else
    echo 1
    return 1
  fi
}

function rmrdir() {
  export HADOOP_HOME=/opt/hadoop/program
  hadoop_bin=$HADOOP_HOME/bin-mapreduce1/hadoop
  $hadoop_bin fs -rmr -skipTrash $2
  date_now=`date +"%Y-%m-%d %H:%M:%S"`
  day=`date +"%Y-%m-%d"`
  echo "$date_now:: file:$1, del path:$2, $hadoop_bin fs -rmr -skipTrash $2" >> get_source_$day.log
}

echo $1
export JAVA_HOME=/usr/lib/jvm/java-6-sun/
hadoop_binary=/opt/hadoop_client/program/bin-mapreduce1/hadoop-pipes
export HADOOP_HOME=/opt/hadoop/program
hadoop_bin=$HADOOP_HOME/bin-mapreduce1/hadoop
hdfs_bin_dir=/tmp
cpp_binary=source
data_dir=$cpp_binary"_data"
time_inter_start=$[3600*24*90]
time_inter_end=$[3600*24*7]
output_base_dir="/production/routine/get_source"
queue="production"
jobname="guanli_hbase-antispam-getsource"
old_days=30
SERVER_IP=10.77.105.147::MINI_SEARCH/pic_path/raw_txt/
ZONE_SERVER_IP=10.77.105.147::MINI_SEARCH/location_path/raw_txt/
SERVER_ODICT_IP=10.73.12.76::MINI_SEARCH/dict

# del old
time_old=`date -d "-$old_days day" +%Y_%m_%d`
old_dirs=`$hadoop_bin fs -ls $output_base_dir/get_source_*`
old_del_dirs=($(echo $old_dirs|awk '{for(i=1;i<=NF;++i){if(match($i,"get_source_")){split($i,array,"__");if(array[2]<"'$time_old'"){print array[1]"__"array[2]"__"array[3]"__"};}}}'|sort|uniq))
for i in ${old_del_dirs[@]};do
  rmrdir $0 $i
  echo rmrdir $0 $i
done

################for i in {1..10};do
time_begin=`date +%s`
time_start_s=`expr $time_begin - $time_inter_start`
time_end_s=`expr $time_begin - $time_inter_end`
time_start=`date -d @$time_start_s +'%Y-%m-%d %H:%M:%S'`
time_end=`date -d @$time_end_s +'%Y-%m-%d %H:%M:%S'`
hbase_query="select cf:* from weibo where datetime>= $time_start and datetime<= $time_end"
echo $hbase_query
echo " start:"$time_start_s", end:"$time_end_s
dir_time=`date +'%Y_%m_%d'`
dir_time_begin=`date -d @$time_start_s +'%Y_%m_%d_%H_%M_%S'`
dir_time_end=`date -d @$time_end_s +'%Y_%m_%d_%H_%M_%S'`
output_subdir="/"$cpp_binary"__"$dir_time"__"$dir_time_begin"_"$dir_time_end"__"
output_dir=$output_base_dir/$output_subdir
#skipped DATA fields
skip_fields="17,id"
# output format : atline, json_style, json_nostyle
output_format='atline'
#fields ini 
fields_file_path="./fields.ini"
#uid list
mapper_num=256
reducer_num=256
format_jar="/opt/hadoop_client/program/share/hadoop/mapreduce1/lib/hadooptool-format.jar"

cmd="
./${cpp_binary} 
--auto_run
--hdfs_bin_dir=$hdfs_bin_dir
--hadoop_binary=$hadoop_binary
--hdfs_host=hdfs://sh-b0-sv0258:8020
--hdfs_server=sh-b0-sv0258.sh.idc.yunyun.com
--num_mapper=${mapper_num}
--num_reducer=${reducer_num}
--output_format=multi_text
--input_format=hbase_sql_combine
--lib_jars=./weibo_format.jar,file:${format_jar}
--pipes_version=2
--map_speculative=false
--reduce_speculative=false
--hbase_config=/opt/hbase/program/conf/hbase-site.xml
--hbase_mr_sql='$hbase_query'
--custom_mapred_params=user.email:wulei2@staff.sina.com.cn^o^mapred.job.name:${jobname}^o^mapred.job.queue.name:$queue^o^mapreduce.outputformat.class:org.apache.hadoop.mapreduce.lib.output.WeiboOutputFormat^o^mapred.textoutputformat.separator:
--uploading_files=${fields_file_path}
--skip_fields_list=${skip_fields}
--multi_sstable_reducer_output_dirs=dup1,dup2
--hdfs_input_paths=weibo
--hdfs_output_dir=${output_dir}
--weibo_export_file_type=$output_format
--fields_file_path='fields.ini'
--last_week=1
--start_source_begin=$time_start_s
--start_source_end=$time_end_s
--last_year=5
"
#--output_format=text
echo $cmd
eval $cmd
##############done

function get_hdfs_data
{
  mkdir -p $data_dir
  rm -rf ./$data_dir/*
  $hadoop_bin fs -get $output_dir ./$data_dir
  cat ./$data_dir/$output_subdir/part-r*|grep mul_source|awk '{print $1}'|awk '{if(NF==1){if($1>0){print $1}}}' > mul_source.txt
  cat ./$data_dir/$output_subdir/part-r*|grep source_1|awk '{if(NF>3){print $1,"\t",$4} else{print $1,"\t",$3}}'|awk '{if(NF==2){if($1>0){print $1,"\t",$2}}}' > source_1.txt
  cat ./$data_dir/$output_subdir/part-r*|grep source_2|awk '{if(NF>3){print $1,"\t",$4} else{print $1,"\t",$3}}'|awk '{if(NF==2){if($1>0){print $1,"\t",$2}}}' > source_2.txt
  cat ./$data_dir/$output_subdir/part-r*|grep source_maxday_suspect|grep zone_suspect|awk '{print $1}'|awk '{if(NF==1){if($1>0){print $1}}}' > source_and_zone_suspect.txt
  cat ./$data_dir/$output_subdir/part-r*|grep mul_zone|awk '{print $1}'|awk '{if(NF==1){if($1>0){print $1}}}' > mul_zone.txt
  cat ./$data_dir/$output_subdir/part-r*|grep zone_1|awk '{if(NF>3){if(length($4)>1){print $1,"\t",$4}} else{if(length($3)>1){print $1,"\t",$3}}}'|awk '{if(NF==2){if($1>0){if(length($2)>1){print $1,"\t",$2}}}}' > zone_1.txt
        
  source1_count=`cat source_1.txt|wc -l`
  if [ $source1_count -lt 10000001 ]; then
    echo "source1_count:"$source1_count" < 10000001" >> get_source.log
    exit 1
  fi
  echo "source1_count:"$source1_count  >> get_source.log
  source2_count=`cat source_2.txt|wc -l`
  if [ $source2_count -lt 1000000 ]; then
    echo "source2_count:"$source2_count" < 1000000" >> get_source.log
    exit 1
  fi
  echo "source2_count:"$source2_count >> get_source.log
  mul_source_count=`cat mul_source.txt|wc -l`
  if [ $mul_source_count -lt 500000 ]; then
    echo "mul_source_count:"$mul_source_count" < 500000" >> get_source.log
    exit 1
  fi
  echo "mul_source_count:"$mul_source_count >> get_source.log
  rsync mul_source.txt source_1.txt source_2.txt source_and_zone_suspect.txt $SERVER_IP
  rsync mul_zone.txt zone_1.txt $ZONE_SERVER_IP
  rsync 10.73.12.132::MINI_SEARCH/guanli/ac_suspect_uid_validfans ./
  rsync 10.73.12.132::MINI_SEARCH/guanli/ac_suspect_uid_set ./
  rm uid_source_dict1*;./ac_suspect_uid_validfans -l 50000000 -i source_1.txt -o uid_source_dict1 -f 0
  rm uid_source_dict2*;./ac_suspect_uid_validfans -l 20000000 -i source_2.txt -o uid_source_dict2 -f 0
  rm uid_source_mul_dict*;./ac_suspect_uid_set -l 10000000 -i mul_source.txt -o uid_source_mul_dict  
  rm uid_source_suspect_dict*;./ac_suspect_uid_set -l 2000000 -i source_and_zone_suspect.txt -o uid_source_suspect_dict
  rsync uid_source_dict1.* uid_source_dict2.* uid_source_mul_dict.* uid_source_suspect_dict.* $SERVER_ODICT_IP
  sleep 2
  touch uid_source_dict1.stamp uid_source_dict2.stamp uid_source_mul_dict.stamp uid_source_suspect_dict.stamp
  rsync uid_source_dict1.stamp uid_source_dict2.stamp uid_source_mul_dict.stamp uid_source_suspect_dict.stamp $SERVER_ODICT_IP
}

get_hdfs_data

###filter_uid_list_file="./vip_user.txt"
###--uploading_files=${fields_file_path},${filter_uid_list_file}
