import constants
import pandas as pd
import datetime
import numpy as np
from google.cloud import bigquery
import os
import json

import time
from cryptography.fernet import Fernet

with open('google.key', 'rb') as file:
    secret = file.read()

fernet = Fernet(os.environ.get('GOOGLE_KEY'))

secret_json = fernet.decrypt(secret)

with open('aw-8a5d408d-02e1-4907-9163-b4d-ed487f09f36b.json', 'w') as json_file:
    json_file.write(secret_json.decode())

##
# Gets the full time range of one variable and one drone
# sub-selected to "buckets" number of points
# Time is returned as a UNIX EPOC seconds value
def get_minmax_timeseries(drone, var, buckets, time_start, time_end):
    client = bigquery.Client()
    preselect = f'''
        WITH Data AS (
            SELECT
                UNIX_SECONDS(TIMESTAMP(time)) AS time_seconds,
                {var} as value
            FROM
                `aw-8a5d408d-02e1-4907-9163-b4d.TPOS.1Hz_2023`
            WHERE trajectory="{drone}" AND time>="{time_start}" AND time<="{time_end}" ORDER BY (time_seconds)
            ),
            BucketedData AS (
            SELECT
                time_seconds,
                value,
                FLOOR((time_seconds - MIN(time_seconds) OVER ()) / (MAX(time_seconds) OVER () - MIN(time_seconds) OVER ()) * {buckets}) AS bucket
            FROM
                Data
            ),
            MinMaxData AS (
            SELECT bucket, min(value) as min_value, max(value) as max_value FROM BucketedData GROUP BY bucket
            ),
            FirstFilter AS (
            SELECT MinMaxData.bucket as bucket, BucketedData.value as value, BucketedData.time_seconds as time_seconds from MinMaxData
            JOIN BucketedData ON MinMaxData.bucket=BucketedData.bucket AND (BucketedData.value=MinMaxData.min_value OR BucketedData.value=MinMaxData.max_value)
            ORDER BY MinMaxData.bucket
            )
            SELECT DISTINCT value, bucket, MIN(time_seconds) as time_seconds FROM FirstFilter
            GROUP BY value, bucket
            ORDER BY time_seconds
    '''
    try: 
        df = client.query(preselect).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return None

##
# Gets the full time range of one variable and one drone
# Time is returned as a UNIX EPOC seconds value
def get_timeseries(drone, var, time_start, time_end):
    client = bigquery.Client()
    query = f'''
        SELECT
            UNIX_SECONDS(TIMESTAMP(time)) AS time_seconds,
            {var} as value
        FROM
            `aw-8a5d408d-02e1-4907-9163-b4d.TPOS.1Hz_2023`
        WHERE trajectory="{drone}" AND time>="{time_start}" AND time<="{time_end}" ORDER BY (time_seconds)
    '''
    try: 
        df = client.query(query).to_dataframe()
        return df
    except Exception as e:
        print(e)
        return None
