#!/usr/bin/env python3
#pyspark --packages org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31
import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument("--catalog", help="catalog location")
parser.add_argument("--table-name", help="table name")
args = parser.parse_args()
if args.table_name:
    table_name = args.table_name
if args.catalog:
    catalog = args.catalog

conf = (
    SparkConf()
        .setAppName('app_name')
  		#packages
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')
  		#Configuring Catalog
        .set('spark.sql.catalog.icebergcat', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.icebergcat.type', 'hadoop')
        .set('spark.sql.catalog.icebergcat.warehouse', 's3a://%s/iceberg' % (catalog))
        #configure S3
        .set('spark.sql.catalog.icebergcat.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
)
    
spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sql("""
  CREATE TABLE icebergcat.%s (
          email STRING, 
          firstName STRING, 
          lastName STRING
          ) 
          USING iceberg
""" % (table_name))