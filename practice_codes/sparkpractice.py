from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName('test').getOrCreate()

my_data = [(1, 'Pujan'), (2, 'Hari')]
my_cols = ["S.No", "Name"]

df = spark.createDataFrame(data=my_data, schema=my_cols)

df.printSchema()

df.show()