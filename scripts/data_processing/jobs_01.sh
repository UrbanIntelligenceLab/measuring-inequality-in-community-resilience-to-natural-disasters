#!/usr/bin/env bash

spark-submit --num-executors 200 --executor-memory 4g --driver-memory 16g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=2g 01_mobility_data_aggregation.py
spark-submit --num-executors 200 --executor-memory 4g --driver-memory 16g --conf spark.yarn.maxAppAttempts=1 --conf spark.executor.memoryOverhead=2g 02_subset_houston_activity.py
hdfs dfs -get <dest_path>/houston_aug_sep_2017 ../../data/
python 03_gzip2csv_houston_data.py