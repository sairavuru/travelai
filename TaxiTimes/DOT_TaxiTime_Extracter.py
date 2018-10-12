#!/usr/bin/env python3.5.2
# -*- coding: utf-8 -*-

'''
Variable Taxi Fuel - DOT data extraction
Author: Sai Ravuru
Dept: Flight Operations Performance Engineering
Date: 09/18/2018
Organization: Allegiant Air
'''

import numpy as np
import pandas as pd
import os

###Read and compile DOT data from crawling through directory files###
def csv_crawler(directory):

    file_count = 0
    for dir, subdir, files in os.walk(directory):
        #print(dir)
        for file in files:
            filename = os.path.join(dir, file)
            print(filename)
            if filename.endswith('.csv'):
                temp_data = pd.read_csv(filename)
                #print(temp_data)
                if file_count == 0:
                    DOT_df = temp_data
                    file_count += 1
                else:
                    DOT_df = DOT_df.append(temp_data)
            else:
                print('No valid files exist!')

    original_row_count = DOT_df.shape[0]
    #Filter out cancelled or diverted flights
    DOT_df = DOT_df[(DOT_df['CANCELLED'] == 0) | (DOT_df['DIVERTED'] == 0)]

    #Filter out any delays
    DOT_df = DOT_df[(DOT_df['CARRIER_DELAY'].isnull()) & (DOT_df['WEATHER_DELAY'].isnull()) & (DOT_df['NAS_DELAY'].isnull()) & (DOT_df['SECURITY_DELAY'].isnull()) & (DOT_df['LATE_AIRCRAFT_DELAY'].isnull())]

    #Filter out NAs in columns of interest
    DOT_df = DOT_df.dropna(subset=['FL_DATE', 'ORIGIN', 'DEST', 'DEP_TIME', 'TAXI_OUT', 'TAXI_IN', 'ARR_TIME'])

    final_row_count = DOT_df.shape[0]
    print(original_row_count - final_row_count, ' rows were filtered out.')

    return DOT_df


###Read and manipulate dataframes###
def DOT_extract(filename):

    DOT_df = pd.read_csv(filename)

    #Structure into datetime
    DOT_df['FL_DATE'] = pd.to_datetime(DOT_df['FL_DATE'], format='%Y-%m-%d')
    DOT_df['MONTH'] = DOT_df['FL_DATE'].dt.month
    DOT_df['YEAR'] = DOT_df['FL_DATE'].dt.year

    #Pad leading zeros to time
    DOT_df['DEP_TIME'] = DOT_df['DEP_TIME'].apply(lambda x: str(int(x)).zfill(4))
    DOT_df['ARR_TIME'] = DOT_df['ARR_TIME'].apply(lambda x: str(int(x)).zfill(4))

    DOT_df['DEP_HOUR'] = DOT_df['DEP_TIME'].apply(lambda x: x[0:2])
    DOT_df['ARR_HOUR'] = DOT_df['ARR_TIME'].apply(lambda x: x[0:2])

    print(DOT_df.dtypes)
    print(DOT_df)

    #Groupby month, origin/destination for modeling Taxi Out/Taxi In times
    DOT_TaxiOut_df = DOT_df.groupby(by=['YEAR', 'MONTH', 'ORIGIN', 'DEP_HOUR'], level=None, as_index=False)['TAXI_OUT'].agg([np.size, np.mean, np.std])
    DOT_TaxiIn_df = DOT_df.groupby(by=['YEAR', 'MONTH', 'DEST', 'ARR_HOUR'], level=None, as_index=False)['TAXI_IN'].agg([np.size, np.mean, np.std])

    # DOT_df.to_csv('Inputs/DOT_data', index=False)
    # DOT_TaxiOut_df.to_csv('Inputs/DOT_TaxiOut.csv')
    # DOT_TaxiIn_df.to_csv('Inputs/DOT_TaxiIn.csv')

    return DOT_df, DOT_TaxiOut_df, DOT_TaxiIn_df



