import argparse
from pyspark import SparkConf
from pyspark.sql import SparkSession

ref = "main"

conf = (
    SparkConf()
        .setAppName('app_name')
  		#packages
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2,software.amazon.awssdk:bundle:2.18.31,software.amazon.awssdk:url-connection-client:2.18.31,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.74.0')
  		#Configuring Catalog
        #.set('spark.sql.catalog.icebergcat', 'org.apache.iceberg.spark.SparkCatalog')
        #.set('spark.sql.catalog.icebergcat.type', 'hadoop')
        #.set('spark.sql.catalog.icebergcat.warehouse', 's3a://%s/iceberg' % (catalog))
        #configure S3
        #.set('spark.sql.catalog.icebergcat.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')
        .set('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        #nessie integration - https://iceberg.apache.org/docs/latest/nessie/
        .set("spark.sql.catalog.nessie.warehouse", "/tmp/nessie/wharehouse")
        .set("spark.sql.catalog.nessie.uri", "http://localhost:19120/api/v1")
        .set("spark.sql.catalog.nessie.ref", ref)
        .set("spark.sql.catalog.nessie.authentication.type","NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
spark._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

spark.sql("select * from nessie.getting_started_table").show()

# when you are ready for maintenance you will need to run stored procedures like the below
# which compacts files taking care of the small file problem slowing queries down
# for more on maintenance https://iceberg.apache.org/docs/latest/maintenance/

spark.sql("""
         CALL nessie.system.rewrite_data_files(
            table => 'nessie.getting_started_table', 
            strategy => 'binpack', 
            options => map('min-input-files','2')
         )
          """)