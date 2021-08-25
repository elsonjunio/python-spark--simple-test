
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

working_directory = 'jars/*'

my_spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri=mongodb://127.0.0.1/test") \
    .config("spark.mongodb.output.uri=mongodb://127.0.0.1/test") \
    .config('spark.driver.extraClassPath', working_directory) \
    .getOrCreate()

people = my_spark.createDataFrame([("JULIA", 50, 1), ("Gandalf", 1000, 2), ("Thorin", 195, 2), ("Balin", 178, 2), ("Kili", 77, 2),
                            ("Dwalin", 169, 2), ("Oin", 167, 2), ("Gloin", 158, 2), ("Fili", 82, 2), ("Bombur", 22, 2)], ["name", "age", "classification_id"])


classification = my_spark.createDataFrame([("REAL", 1), ("FICTION", 2)], ["name", "id"])


people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("collection", "people").save()

classification.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("collection", "classification").save()


people_df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "people").load()
classification_df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "classification").load()


people_df.select('*').where(col("name") == "JULIA").show()
classification_df.select('*').where(col("name") == "REAL").show()

people_df.filter(people_df['age'] >= 10).show()
classification_df.filter(classification_df['id'] >= 1).show()

people_df.createOrReplaceTempView("people")
classification_df.createOrReplaceTempView("classification")


some_registers = my_spark.sql("SELECT name, age FROM people WHERE name LIKE '%h%'")
some_registers.show()

sum_registers = my_spark.sql("SELECT SUM(age) FROM people WHERE name LIKE '%h%'")
sum_registers.show()

some_registers = my_spark.sql("SELECT name, id FROM classification WHERE name LIKE '%R%'")
some_registers.show()

some_registers_join = my_spark.sql("SELECT * FROM people p JOIN classification c ON p.classification_id == c.id")
some_registers_join.show()
