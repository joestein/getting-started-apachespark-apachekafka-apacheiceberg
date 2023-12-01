#!/usr/bin/env python3
import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession
from confluent_kafka.schema_registry import SchemaRegistryClient
import os

parser = argparse.ArgumentParser()

parser.add_argument("--table-name", help="table name")
args = parser.parse_args()
if args.table_name:
    table_name = args.table_name

ref = "main"

conf = (
    SparkConf()
        .setAppName('app_name')
  		#packages
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.73.0')
        #nessie integration - https://iceberg.apache.org/docs/latest/nessie/
        .set("spark.sql.catalog.nessie.warehouse", "s3a://miniotesting")
        .set("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")
        .set("spark.sql.catalog.nessie.ref", ref)
        .set("spark.sql.catalog.nessie.authentication.type","NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint","localhost:9000") #configure minio endpoint
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access","true")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key",os.environ['AWS_ACCESS_KEY_ID'])
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key",os.environ['AWS_SECRET_ACCESS_KEY'])
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")


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
.start(f"nessie.{table_name}")

writer.awaitTermination()