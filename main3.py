from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

working_directory = 'jars/*'

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri=mongodb://192.168.100.40:27017") \
    .config("spark.mongodb.input.database=test") \
    .config("spark.mongodb.input.collection=myCollection") \
    .config("spark.mongodb.output.uri=mongodb://192.168.100.40:27017") \
    .config("spark.mongodb.output.database=test") \
    .config("spark.mongodb.output.collection=myCollection") \
    .config('spark.driver.extraClassPath', working_directory) \
    .getOrCreate()

people = my_spark.createDataFrame([("JULIA", 50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
                            ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", 22)], ["name", "age"])


people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("uri", "mongodb://192.168.100.40:27017").option("database", "test").option("collection", "myCollection").save()

df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", "mongodb://192.168.100.40:27017").option("database", "test").option("collection", "myCollection").load()

df.createOrReplaceTempView("temp")
some_registers = my_spark.sql("SELECT name, age FROM temp WHERE name LIKE '%h%'")

sum_registers = my_spark.sql("SELECT SUM(age) FROM temp WHERE name LIKE '%h%'")