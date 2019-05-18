#!/bin/bash

###### CONSTANT VARIABLES #######
hdfs_input_path="../datasets"
statistical_operations_jar="./five_operations.jar"
#################################

###### STATIC VARIABLES #########
bug_counter=0
#################################

debug_command(){
	echo "[$bug_counter] COMMAND IS EXECUTED: <<$1>>"
	echo "OUTPUT: "
	($1)
	echo "--------------------------------------------------------"
	let bug_counter=$bug_counter+1
}

choose_operation(){
	echo "<<$1>>"
	if [ $1 -eq "0" ];then operation="Sum_Operation";
		elif [ $1 == "1" ];then operation="Mean_Operation";
		elif [ $1 == "2" ];then operation="StdDev_Operation";
		elif [ $1 == "3" ];then operation="Range_Operation";
		elif [ $1 == "4" ];then operation="Median_Operation"; 
		else
			echo "ERROR: Operation numbers must be 0~4"
			echo "Exiting.." && exit -1	
	fi
}

main(){
	input_file=$1
	input_file_name=${input_file##*/}
	output_dir=$2
	
	choose_operation $3

	debug_command "start-dfs.sh"
	debug_command "start-yarn.sh"	
	debug_command "jps"
	debug_command "hdfs dfs -mkdir -p $hdfs_input_path"
	debug_command "hdfs dfs -put $input_file $hdfs_input_path"
	# debug_command "hadoop fs -rmr $output_dir"
	debug_command "hadoop jar $statistical_operations_jar bigdataproject.$operation $hdfs_input_path/$input_file_name $output_dir"
	debug_command "hadoop fs -cat $output_dir/part-r-00000"

	echo "Bash Script is executed succesfully."
}
# $1: Input File
# $2: Output Path
# $3: Chosen Operation Numerically {1:sum,2:mean,3:std,4:range,5:median} 
main "$1" "$2" "$3"


