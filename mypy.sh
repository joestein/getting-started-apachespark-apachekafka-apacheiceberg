#!/bin/bash
set -e

pyspark --packages org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.71.0 --conf spark.sql.extensions="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions"