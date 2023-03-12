#!/usr/bin/env python
# coding: utf-8

import argparse

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


parser = argparse.ArgumentParser()

parser.add_argument('--input_fhv', required=True)
parser.add_argument('--output', required=True)

args = parser.parse_args()

input_fhv = args.input_fhv
output = args.output


spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()

df_fhv = spark.read.parquet(input_fhv)

common_colums = [
    'dispatching_base_num',
    'pickup_datetime',
    'dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'SR_Flag',
    'Affiliated_base_number'
]

df_fhv.registerTempTable('trips_data')

df_result = spark.sql("""
SELECT 
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,
    COUNT(*) AS trips_count
FROM
    trips_data
GROUP BY
    1, 2
""")


df_result.coalesce(1) \
    .write.parquet(output, mode='overwrite')