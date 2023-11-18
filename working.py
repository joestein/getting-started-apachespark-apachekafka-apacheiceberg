#pyspark --master k8s://https://74E303401DE9A345BCA32F9C1285593C.gr7.us-east-2.eks.amazonaws.com:443 --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.2.2  --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" --conf "spark.kubernetes.container.image=apache/spark-py:v3.4.0"

#delta
#pyspark --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

#delta and iceberg maybe remove delta
#pyspark --packages io.delta:delta-spark_2.12:3.0.0,org.apache.hadoop:hadoop-aws:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.spark:spark-avro_2.12:3.5.0,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31 --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

spark = SparkSession.builder \
    .appName('application') \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()
#.config("spark.sql.warehouse.dir", "s3://spark-meson-metastore/") \
    
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
#spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "eJ3mSFh2DtpUQXEi3t3I")
#spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "3lXJ3IwmSi4zD05UzUmXRP6rbZzUGPWOTXC3GtDh")
#spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://localhost:50123")
#spark._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled', 'false')
#spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
#spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3-website.us-east-2.amazonaws.com")

table = "table13"
spark.sql("""
  CREATE TABLE %s (
          email STRING, 
          firstName STRING, 
          lastName STRING
          ) 
          USING delta 
          PARTITIONED BY (lastName) 
          LOCATION 's3a://anothertester2/%s}'
""" % (table, table))

spark.sql("""
  INSERT INTO %s VALUES
      ('joe.stein@gmail.com', 'Joe', 'Stein')
""" % (table))

a = spark.sql("SELECT * FROM %s" % (table))
print(a.toPandas()["country"])

from confluent_kafka.schema_registry import SchemaRegistryClient

kafkaBootstrapServers="localhost:9092"
topicName = "my-topic"
schemaRegistryUrl="http://localhost:8081"
schema_registry_conf = {
    'url': schemaRegistryUrl}#,
    #'basic.auth.user.info': '{}:{}'.format(schemaRegistryApiKey, schemaRegistrySecret)}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)
my_schema_response = schema_registry_client.get_latest_version(topicName + "-value").schema
my_schema = my_schema_response.schema_str

streamDF = ( 
  spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafkaBootstrapServers)
  #.option("kafka.sasl.jaas.config", "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='{}' password='{}';".format(kafkaUser, kafkaSecret))
  #.option("kafka.security.protocol", "SASL_SSL")
  #.option("kafka.sasl.mechanism", "PLAIN")
  .option("subscribe", topicName)
  .option("startingOffsets", "latest")
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

itemDF.writeStream \
.format("console") \
.outputMode("append") \
.start() \
.awaitTermination()

itemDF.writeStream \
.format("delta") \
.outputMode("append") \
.option("mergeSchema", "true") \
.option("checkpointLocation", "/tmp/kafka") \
.start("s3a://anothertester2/table13")

itemDF.writeStream \
.format("iceberg") \
.outputMode("append") \
.option("mergeSchema", "true") \
.option("checkpointLocation", "/tmp/kafka-iceberg") \
.start("s3a://anothertester2/iceberg/icebergtable2")

path_to_data = 's3a://anothertester2/table13'
df = spark.read.format("delta").load(path_to_data)
df.createOrReplaceTempView("dbtable")
df.sql("select * from dbtable").show()

#### ICEBERG

conf = (
    pyspark.SparkConf()
        .setAppName('app_name')
  		#packages
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.1.0,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31')
  		#SQL Extensions
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
  		#Configuring Catalog
        .set('spark.sql.catalog.icebergcat', 'org.apache.iceberg.spark.SparkCatalog')
        .set('spark.sql.catalog.icebergcat.type', 'hadoop')
        .set('spark.sql.catalog.icebergcat.warehouse', 's3a://anothertester2/iceberg')
        .set('spark.sql.catalog.icebergcat.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
)

#.config("spark.sql.warehouse.dir", "s3://spark-meson-metastore/") \
spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")


table = "icebergtable2"
spark.sql("""
  CREATE TABLE icebergcat.%s (
          email STRING, 
          firstName STRING, 
          lastName STRING
          ) 
          USING iceberg
""" % (table))

spark.sql("""
  INSERT INTO icebergcat.%s VALUES
      ('joe.stein@gmail.com', 'Joe', 'Stein')
""" % (table))

a = spark.sql("SELECT * FROM icebergcat.%s" % (table))
print(a.toPandas()["country"])

path_to_data = 's3a://anothertester2/iceberg/icebergtable2'
df = spark.read.format("iceberg").load(path_to_data)
df.createOrReplaceTempView("dbtable")
spark.sql("select * from dbtable").show()


## Start Spark Session
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#from deltalake import DeltaTable
#dt = DeltaTable("/tmp/deltalake_data4")

#dt.to_pandas(partitions=[("country", "=", "china")], columns=["continent"])
s3a://localhost:50123/
eJ3mSFh2DtpUQXEi3t3I
3lXJ3IwmSi4zD05UzUmXRP6rbZzUGPWOTXC3GtDh

export AWS_ACCESS_KEY="AKIA4M6CLXYJBQI3V6HJ"
export AWS_SECRET_KEY="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"

.config("spark.hadoop.fs.s3a.access.key", "AKIA4M6CLXYJBQI3V6HJ") \
.config("spark.hadoop.fs.s3a.secret.key", "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") \
    