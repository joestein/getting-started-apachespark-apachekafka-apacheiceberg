#!/bin/bash
set -e

export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

python3 create_bucket.py

spark-submit --packages org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.73.0 create_table.py --table-name=$1