# pyspark_test

Create a new venv:

```
$ python3 -m venv venv
$ source venv/bin/activate
```

Download jars

```
$ mkdir jars
$ cd jars
$ wget https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/2.4.0/mongo-spark-connector_2.12-2.4.0.jar
$ wget https://repo1.maven.org/maven2/org/mongodb/mongo-java-driver/3.11.2/mongo-java-driver-3.11.2.jar
```

See more in https://docs.mongodb.com/spark-connector/current/python-api/