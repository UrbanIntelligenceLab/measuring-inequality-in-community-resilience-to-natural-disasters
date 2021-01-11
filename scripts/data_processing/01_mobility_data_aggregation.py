#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" 
Author: Bartosz Bonczak
Title: 01_mobility_data_aggregation

Description:
This code is designed to simplify and reduce both spatial and temporal 
resolution of the raw, annonymized mobility data provided by VenPath Inc. 
in order further reduce the risk of re-identification of the individual 
device and to normalize ping frequency in various application types. It 
is achieved by the following steps:
- standardize device's uniqe and annonymous adevrtisement ID (ad_id) 
information by using upper case 
- aggregating mobility data by 5-minute interval and reporting  average 
latitude and longitude
"""

# Imports
from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql.functions import mean, window, upper, col, count, round, collect_set
from pyspark.sql.types import * # import types


if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	data_path = ""   # specify path to the data
	dest_path = "" # specify output path

	# Specify schema
	schema = StructType([
		StructField("id", LongType(), True),
		StructField("app_id", StringType(), True),
		StructField("ad_id", StringType(), True),
		StructField("id_type", StringType(), True),
		StructField("country_code", StringType(), True),
		StructField("device_make", StringType(), True),
		StructField("device_model", StringType(), True),
		StructField("device_os", StringType(), True),
		StructField("device_os_version", StringType(), True),
		StructField("latitude", DoubleType(), True),
		StructField("longitude", DoubleType(), True),
		StructField("timestamp", TimestampType(), True),
		StructField("ip_address", StringType(), True),
		StructField("horizontal_acc", DoubleType(), True),
		StructField("vertical_acc", StringType(), True),
		StructField("foreground", BooleanType(), True),
	])

	# load data
	df = sqlContext.read.format("csv") \
		.schema(schema) \
		.option("header", "false") \
		.load(data_path)

	# standardize ad_id 
	df = df.withColumn('ad_id_upper', upper(col('ad_id')))
		
	# aggregate data
	agg = df.groupby(
		window("timestamp", "5 minutes"), "ad_id_upper").agg( \
			mean("longitude").alias("lon_avg"), \
			mean("latitude").alias("lat_avg"), \
			count("id").alias("ping_counts"), \
			collect_set("country_code")[0].alias("country"), \
			collect_set("device_make")[0].alias("make")
	)

	# save to parquet files
	agg.select(
		agg.window.start.cast("string").alias("time_start"), \
		agg.window.start.cast("date").alias("date"),\
		col("ad_id_upper").alias("ad_id"), \
		round(col("lon_avg"), 6).alias("lon_avg"), \
		round(col("lat_avg"), 6).alias("lat_avg"), \
		"ping_counts", "country", "make") \
	.write.partitionBy("date") \
	.mode("overwrite") \
	.parquet("{}/daily_5min_agg_parquet".format(dest_path))			