
from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf
spark = SparkSession.builder.appName("MyApp").getOrCreate()
df = spark.createDataFrame([(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"])
df.show()
sc = spark.SparkContext()

rdd = sc.parallelize([1,2,3])
count = rdd.count()
print(sc.master)
print(count)

# Load the data from its source.
df = spark.read.load("/databricks-datasets/learning-spark-v2/people/people-10m.delta")

# Write the data to a table.
table_name = "people_10m"
df.write.saveAsTable(table_name)