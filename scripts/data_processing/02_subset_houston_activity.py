#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" 
Author: Bartosz Bonczak
Title: 02_subset_houston_activity

Description:
This code snippet generates a subset of mobility data within the Houston, 
TX area defined by a bounding box and covering two months period of 
August and September 2017.
"""

## Imports
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark import SparkFiles

from pyspark.sql.functions import col
from pyspark.sql.types import * # import types

if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	data_path = ""   # specify data path
	dest_path = "" # specify output destination path

	# load aggregated data
	df = sqlContext.read.parquet('{}/daily_5min_agg_parquet/'.format(data_path))

	# filter data to represent pings from August and September 2017 
	# within the Houston area and remove randomized devices
	subset = df.filter(
		(col('latitude')>=-96.0266) \
		& (col('latitude')<=-94.8285) \
		& (col('longitude')>=29.4492) \
		& (col('longitude')<=30.2409) \
		& (col('year')==2017) \
		& (col('month').isin([8,9]) )
		& (col('ad_id')!='00000000-0000-0000-0000-000000000000') \
	)

	# save as compressed CSV files for further processing
	subset.select("time", "ad_id", "latitude", "longitude", "count") \
	.coalesce(200).write \
	.format("com.databricks.spark.csv") \
    .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
    .save("{}/houston_aug_sep_2017/".format(dest_path))

    # or alternatively save as parquet files for easier processing in PySpark
    subset.select("time", "ad_id", "latitude", "longitude", "count") \
    .mode("overwrite") \
    .write \
    .parquet("{}/houston_aug_sep_2017_parquet/".format(dest_path))