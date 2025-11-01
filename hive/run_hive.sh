#!/bin/bash

if [ "$#" -ne 3 ]; then
  echo "Użycie: $0 <input_dir3> <input_dir4> <output_dir6>"
  exit 1
fi

input_dir3=$1
input_dir4=$2
output_dir6=$3

hdfs dfs -test -e $output_dir6

if [ $? -eq 0 ]; then
    hdfs dfs -rm -r -skipTrash $output_dir6
fi

beeline -n "$(id -un)" -u "jdbc:hive2://localhost:10000/default" \
  --hivevar input_dir3=$input_dir3 \
  --hivevar input_dir4=$input_dir4 \
  --hivevar output_dir6=$output_dir6 \
  -f hive.hql