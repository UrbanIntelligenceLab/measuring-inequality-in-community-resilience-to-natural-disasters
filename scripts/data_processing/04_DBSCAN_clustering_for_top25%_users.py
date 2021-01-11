#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" 
Author: Bartosz Bonczak
Title: 04_DBSCAN_clustering_for_top25%_users

Description:
Process mobility data to select to 25% of most active users during the study 
period and perform DBSCAN cluster analysis to identify clusters of activity.

packages:
Pandas (version='0.25.0')
Numpy (version='1.15.4')
scikit-learn (version='0.20.1')
"""

## imports
from pyspark import SparkContext
from pyspark.sql import SQLContext

from pyspark.sql.functions import mean, col, count, min, max, to_date, lit, \
expr, hour, minute, from_utc_timestamp, pandas_udf, PandasUDFType
from pyspark.sql.types import * # import types

from sklearn import cluster
import sklearn.preprocessing as pp

import numpy as np
import pandas as pd
from math import sin, cos, sqrt, atan2, radians


def distance_km(x1, y1, x2, y2):
	# calculates the activity geographical extent 
	# approximate radius of earth in km
	R = 6373.0
	lat1 = radians(y1)
	lon1 = radians(x1)
	lat2 = radians(y2)
	lon2 = radians(x2)
	dlon = lon2 - lon1
	dlat = lat2 - lat1
	a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
	c = 2 * atan2(sqrt(a), sqrt(1 - a))
	distance = R * c
	return distance

def define_min_sample(df):
	# specify min sample based on activity size
	if len(df) < 288:
	    min_sample = 3
	else:
	    min_sample = len(pd.unique(df.time_CST.dt.date)) + 1
	return min_sample

def cluster_char(subset_df):
	# function to calculate various characteristics of the clusters
	ad_id = subset_df['ad_id'].unique()[0]
	size = len(subset_df)
	avg_lon = subset_df.lon.mean()
	avg_lat = subset_df.lat.mean()
	dates = np.array2string(np.sort(subset_df.time_CST.dt.date \
		.apply(lambda x: x.strftime('%Y-%m-%d'))\
		.unique().tolist()), separator=',')
	hours = np.array2string(
		np.histogram(subset_df.hour, bins=24)[0], separator=',')
	return pd.Series({
		'ad_id':ad_id, 'size':size, 'lon':avg_lon, 'lat':avg_lat, 
		'dates_active':dates, 'hours_active':hours})

if __name__ == "__main__":
	sc = SparkContext()
	sqlContext = SQLContext(sc)

	data_path = ""   # specify data path
	dest_path = "" # specify output destination path

	# 1. GENERATE USER'S ACTIVITY SUMMARY
	# load 5-minute aggregated mobility data
	df = sqlContext.read.parquet('{}/daily_5min_agg_parquet'.format(data_path))

	# describe each ad_id to understand device activity
	all_users = df.groupby("ad_id") \
	.agg( \
		count("time").alias("active"), \
		min("time").alias("first"), \
		max("time").alias("last"), \
		mean("longitude").alias("lon"), \
		mean("latitude").alias("lat")\
	).sort(col("active").desc())

	# 2. SUBSET 25% OF OVERALL MOST ACTIVE DEVICES PRESENT IN HOUSTON DURING 
	# THE STUDY PERIOD
	# load Houston data
	houston = sqlContext.read\
	.parquet('{}/}houston_aug_sep_2017_parquet/'.format(data_path))

	# find unique ad_id in Houston and join with user activity summary
	houston_users = houston.select('ad_id').distinct()\
	.withColumnRenamed("ad_id", "id")
	
	subset_users = all_users\
	.join(houston_users, all_users.ad_id == houston_users.id, how='inner')

	# ensure users first and last activity happened before and after impact 
	# period respectively
	date_from, date_to = [to_date(lit(s)).cast(TimestampType()) \
		for s in ("2017-08-21", "2017-08-31")]
	subset_users = subset_users.filter(\
		(subset_users.first <= date_from) \
		& (subset_users.last >= date_to))

	# select top 25% of active users
	active_75percentile = subset_users.agg(
		expr('percentile(active, array(0.75))')[0]\
		.alias('%75')).collect()[0]['%75']
	subset_users = subset_users.filter(\
		subset_users.active > active_75percentile)

	# 3. FIND ALL ACTIVITY OF TOP 25% ACTIVE USERS
	houston_valid_users = df.join(subset_users.select('id'), \
		df.ad_id == subset_users.id, how='inner')

	# 4. PARSE TIME INFORMATION
	# convert time to US Cental time zone and create 5 min index (0-288)
	houston_valid_users = houston_valid_users \
	.withColumn('time_CST', from_utc_timestamp(col('time'), 'US/Central')) \
	.withColumn('hour', hour(col('time_CST'))) \
	.withColumn('minute', minute(col('time_CST'))) \
	.withColumn('t', col('hour')*12 + col('minute')/5)


	# 5. DEFINE DBSCAN OUTPUT SCHEMA
	schema = StructType([
		StructField("labels", IntegerType(), True),
		StructField("ad_id", StringType(), True),
		StructField("lat", DoubleType(), True),
		StructField("lon", DoubleType(), True),
		StructField("size", IntegerType(), True),
		StructField("dates_active", StringType(), True),
		StructField("hours_active", StringType(), True)
	])

	# 6. DEFINE DBSCAN FUNCTION
	@pandas_udf(schema, PandasUDFType.GROUPED_MAP)
	def analyze_user(sample_pd):
		try:
			# find bounding box corners
			xy1 = (sample_pd.lon.min(), sample_pd.lat.min())
			xy2 = (sample_pd.lon.max(), sample_pd.lat.max())
			# find coordintaes of middle bounding box extent
			xx = [(xy1[0], np.mean([xy1[1], xy2[1]])), \
				(xy2[0], np.mean([xy1[1], xy2[1]]))]
			yy = [(np.mean([xy1[0], xy2[0]]), xy1[1]), \
				(np.mean([xy1[0], xy2[0]]), xy2[1])]
			# calculate length and width of a bounding box in km
			distances = [xx, yy, 288]
			cols = ['lon', 'lat', 't']
			d = dict(zip(cols, distances))
			# create lat-lon-time 2D array
			X = sample_pd[['lon', 'lat', 't']].copy()
			# re-scale values
			for c in X.columns:
				if c == 't':
					X[c] = pp.minmax_scale(X[c], feature_range=(0, 1))
				else:
					X[c] = pp.minmax_scale(X[c], \
						feature_range=(0, distance_km(d[c][0][0], 
							d[c][0][1], d[c][1][0], d[c][1][1])))
			X = np.array(X)
			min_sample = define_min_sample(sample_pd)
			dbscan = cluster.DBSCAN(eps=0.25, min_samples=min_sample).fit(X)
			sample_pd['labels'] = dbscan.labels_
			clusters = sample_pd.groupby(['labels'])\
				.apply(cluster_char).reset_index()
			clusters = clusters[clusters['labels']!=-1]\
				.sort_values(by='size', ascending=False)
			clusters['labels'] = range(1, len(clusters)+1)
			return clusters
		except:
			ad_id = subset_df['ad_id'].unique()[0]
			return pd.DataFrame({'labels':[0], 'ad_id':[ad_id], \
				'lat':[0], 'lon':[0], 'size':[0], \
				'dates_active':[''], 'hours_active':['']})


	# apply DBSCAN for each user
	user_df = houston_valid_users.groupby('ad_id')\
	.apply(analyze_user)

	# save to CSV file
	user_df.repartition(200) \
	.write.format("csv") \
	.option("header", True) \
	.mode("overwrite") \
	.save('{}/clustering_02_2020'.format(dest_path))