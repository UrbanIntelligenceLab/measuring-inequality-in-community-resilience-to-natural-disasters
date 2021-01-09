#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" 
Author: Bartosz Bonczak
Title: 03_gzip2csv_houston_data

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
data_path = '../../data/houston_aug_sep_2017/'

# generate list of files in a directory
onlyfiles = [f for f in listdir(data_path) if isfile(join(data_path, f))]

# initiate Pandas DataFrame
df = pd.DataFrame()

# populate dataframe with file content
for f in onlyfiles[1:]:
    temp = pd.read_csv('{}{}'.format(data_path, f), \
    	compression='gzip', \
    	header=None, sep=',', \
    	quotechar='"')
    df = df.append(temp)

# rename column names
df.columns = ['timestamp', 'ad_id', 'lon', 'lat', 'activity']

# save
df.to_csv('../../data/houston_aug_sep_2017.csv', index=False)
