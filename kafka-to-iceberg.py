#!/usr/bin/env python3
#pyspark --packages org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31
import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession
from confluent_kafka.schema_registry import SchemaRegistryClient

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
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')
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

location = "s3a://%s/iceberg/%s" % (catalog, table_name)
checkpoints = "/tmp/kafka-iceberg/%s" % (table_name)

kafkaBootstrapServers="localhost:9092"
topicName = "my-topic"
schemaRegistryUrl="http://localhost:8081"
schema_registry_conf = {
    'url': schemaRegistryUrl}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
my_schema_response = schema_registry_client.get_latest_version(topicName + "-value").schema
my_schema = my_schema_response.schema_str

streamDF = ( 
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  .option("subscribe", topicName)
  .option("startingOffsets", "earliest")
  .load()
)

from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as fn

from_avro_options= {"mode":"PERMISSIVE"}

itemDF = (
  streamDF
  .select(from_avro(fn.expr("substring(value, 6, length(value)-5)"), my_schema, from_avro_options).alias("value"))
  .selectExpr("value.email", "value.firstName", "value.lastName") \
)

writer = itemDF.writeStream \
.format("iceberg") \
.outputMode("append") \
.option("mergeSchema", "true") \
.option("checkpointLocation", checkpoints) \
.start(location)

writer.awaitTermination()