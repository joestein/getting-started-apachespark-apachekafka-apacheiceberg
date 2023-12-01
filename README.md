# getting-started-apachespark-apachekafka-apacheiceberg
Getting Started Apache Spark, Apache Kafka, Apache Iceberg

This code is from the post [here](https://hello.bitsnbytes.world/2023/11/19/getting-started-with-apache-spark-apache-kafka-and-apache-iceberg/).

It then included an integration with the [Nessie](https://projectnessie.org/) Catalog written up in this [post](https://hello.bitsnbytes.world/2023/11/19/data-lake-branch-tag-and-commit-log-history-with-project-nessie/)

I then updated it for [minio](https://min.io/) posted [here](https://hello.bitsnbytes.world/2023/12/01/apache-spark-apache-kafka-apache-iceberg-nessie-and-minio/)

# Up and Running

First make sure you have Python3, Java, Docker and Spark installed.

The Spark [download](https://spark.apache.org/downloads.html). Then to install Spark, extract it in a directory and set that directory as an environment variable called SPARK_HOME and then but the bin folder of SPARK_HOME in your PATH. Let me know if I need to add docs for Python, Java and Docker.

Next you will need some terminals open.

In the first terminal lets start install python libs and startup Kafka, Schama Repo, Nessie and Minio.

```
pip3 install -r requirements.txt
docker-compose up
```

Now in another terminal lets send a message to Kafka.

```
./send.sh
```

In another terminal lets create our Iceberg table managed by the Nessie catalog and store data on Minio.

```
./create_table.sh --table_name=your_table_name
```

In the same terminal lets start up a Spark job to read from Kafka, understand the Avro schema and save that to the Iceberg table we created.

```
./kafka-to-iceberg.sh --table_name=your_table_name
```

Now lets see whats in the table we created. Go to another terminal and run pyspark to run interactive queries.

```
./mypy.sh
```

Now go to the queries.py file and copy all of the imports and configuration. After that you should be able to run your query and see the record you sent to Kafka in the Iceberg table.

```
spark.sql("select * from nessie.your_table_name").show()
```

You can send again and run the select and see it show up. 

Thanx =8^) Joe Stein<br>
[https://www.twitter.com/charmalloc](https://www.twitter.com/charmalloc)<br>
[https://www.linkedin.com/in/charmalloc](https://www.linkedin.com/in/charmalloc)