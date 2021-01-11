#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" 
Author: Bartosz Bonczak
Title: 05_gzip2csv_clusters_data

Description:
Combines fileparts into a single csv file.

packages:
Pandas (version='0.20.3')
"""

## imports
import pandas as pd
from os import listdir
from os.path import isfile, join

# define files path
data_path = '../../data/DBSCAN_clustering/'

# generate list of files in a directory
onlyfiles = [f for f in listdir(data_path) if isfile(join(data_path, f))]

# initiate Pandas DataFrame
df = pd.DataFrame()

# populate dataframe with file content
for f in onlyfiles:
    try:
        temp = pd.read_csv(f)
        df = df.append(temp)
    except:
        pass

# parse timestamp column
df.dates_active = [x.replace('[','').replace(' ', '').replace('\n', '').replace(']','').replace("'","").split(',') for x in df.dates_active]
df.hours_active = [x.replace('[','').replace(' ', '').replace('\n', '').replace(']','').replace("'","").split(',') for x in df.hours_active]

# save
df.to_csv('../../data/clusters_top25_users.csv', index=False)