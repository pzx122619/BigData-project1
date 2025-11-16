#!/bin/bash

if [ "$#" -ne 2 ]; then
  echo "Użycie: $0 <input_dir1> <output_dir3>"
  exit 1
fi

input_dir1=$1
output_dir3=$2

hdfs dfs -test -e $output_dir3

if [ $? -eq 0 ]; then
    hdfs dfs -rm -r -skipTrash $output_dir3
fi

hadoop jar project1.jar $input_dir1 $output_dir3