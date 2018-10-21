#!/bin/bash -
# hbase_weibo_export.sh
# Export Weibo From Hbase
# yangming5
# 2015-06-17
#

current_path=$(cd `dirname $(which $0)`; pwd)
project_home="${current_path}/.."
source ${project_home}/helper/util.sh
source ${project_home}/conf/tools.ini ${project_home}

function usage() {
cat<<EOF
Usage: 
$0 -h | (-s|--start_time) <time> (-e|end_time) <time> (-o|--output_path) <output> [(-d|--doc_action) <action>] [(-q|--task_queue) <name>] [(-n|--task_name) <name>] [(-a|--filter_accept) <fields>] [(-r|--filter_refused) <fields>] [(-c|--compress) <true|false>] [(-m|--match_expression) <expression>]

Options:
-h|--help			Usage help information
-s|--start_time	<time>		Start time of weibo, day: 2015-01-25 or hour: "2015-01-25 11"
-e|--end_time <time>		End time of weibo
-o|--output_path <output>	Mapreduce task output path
-d|--doc_action <action>	Add action value while export
-q|--task_queue <queue>		Mapreduce task queue name
-n|--task_name <name>		Mapreduce task name
-a|--filter_accept <fields>	Only export this fields, Format: field_1#field_2#...
-r|--filter_refused <fields>	Don't export this fields, Format: field_1#field_2#...
-c|--compress <true or false>	Compression enable or disable:[true|false]
-m|--match_expression <express>	Only extract the weibo which fields matched with given expression
				Format: field1,op1,value1#field2,op2,value2#...
				Supported opeartor: 
				 1) EQ:	 equal;
				 2) NEQ: not equal;
				 3) GT:  greater than;
				 4) NGT: not greater equal;
				 5) GE:	 greater equal;
				 6) LT:	 less than;
				 7) NLT: not less than;
				 8) LE:	 less equal;
				 9) CO:  contain substring;
				10) NCO: not contail substring;
				11) PT:  java regex pattern true, not supported yet this version;
				12) NPT: java regex pattern false, not supported yet this version;
				13) MT:  bit mask true;
				14) NMT: bit mask false;
EOF
}

start_time=""
end_time=""
output_path=""
optional_args=""

TEMP=`getopt -o hs:e:o:d:q:n:c:a:r:m: --long help,start_time:,end_time:,output_path:,doc_action:,task_queue:,task_name:,filter_accept:,filter_refused:,compress:,match_expression: -- "$@"`
[ $? != 0 ] && echo -e "\033[31mERROR: unknown argument! \033[0m\n" && usage && exit 1
eval set -- "$TEMP"

while :
do
	[ -z "$1" ] && break;
	case "$1" in
	-s|--start_time) 
		start_time=$2; shift 2
		;;
	-e|--end_time)
		end_time=$2; shift 2
		;;
	-o|--output_path)
		output_path=$2; shift 2
		;;
	-d|--doc_action)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}action${PAYLOAD_LV2_SEP}$2"; shift 2
		;;
	-q|--task_queue)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}queue${PAYLOAD_LV2_SEP}$2"; shift 2
                ;;
	-n|--task_name)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}name${PAYLOAD_LV2_SEP}$2"; shift 2
                ;;
	-a|--filter_accept)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}filter_in${PAYLOAD_LV2_SEP}${2//#/${PAYLOAD_LV3_SEP}}"; shift 2
                ;;
	-r|--filter_refused)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}filter_out${PAYLOAD_LV2_SEP}${2//#/${PAYLOAD_LV3_SEP}}"; shift 2
                ;;
	-c|--compress)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}compress${PAYLOAD_LV2_SEP}$2"; shift 2
                ;;
	-m|--match_expression)
		optional_args="${optional_args}${PAYLOAD_LV1_SEP}value_filter${PAYLOAD_LV2_SEP}${2//#/${PAYLOAD_LV3_SEP}}"; shift 2
                ;;
	-h|--help)
		usage; exit 0
		;;
	--)
		shift
		;;
	*)
		echo -e "\033[31mERROR: unknown argument! \033[0m\n" && usage && exit 1
		;;
	esac
done

if [ -z "$start_time" ]; then
	echo "start_time should not empty"
	exit 1
fi

if [ -z "$end_time" ]; then
        echo "end_time should not empty"
        exit 1
fi

if [ -z "$output_path" ]; then
	echo "output shuld not empty"
	exit 1
fi
	
TASK_CLASS_NAME=ShCdh4HdfsWeiboFieldsExtract
hadoop_payloads="start${PAYLOAD_LV2_SEP}${start_time}"
hadoop_payloads="${hadoop_payloads}${PAYLOAD_LV1_SEP}end${PAYLOAD_LV2_SEP}${end_time}"
hadoop_payloads="${hadoop_payloads}${PAYLOAD_LV1_SEP}output${PAYLOAD_LV2_SEP}${output_path}"
hadoop_payloads="${hadoop_payloads}${optional_args}"

hadoop_opts="-submitByName ${TASK_CLASS_NAME}"
#hadoop_opts="${hadoop_opts} -mapperNum 120"
hadoop_opts="${hadoop_opts} -payLoad \"${hadoop_payloads}"\"

cmd="${HADOOP_BIN} jar ${JAR_PATH} ${hadoop_opts}"
r_log "${cmd}"
eval ${cmd}
