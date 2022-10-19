from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, explode


# Load data
spark = SparkSession.builder.appName('twitter').getOrCreate()

spark.read.format('json').load('data/tweets.json').schema

manual_schema = StructType([
    StructField('country', StringType(), True),
    StructField('id', StringType(), True),
    StructField('place', StringType(), True),
    StructField('text', StringType(), True),
    StructField('user', StringType(), True)
])

twitter_df = spark.read.format('json').schema(manual_schema).load('data/tweets.json')

twitter_df.printSchema()

## 1. Find all the tweets by user
user = 'Daniel Beer'
twitter_df.filter(col('user')==user).show()


## 2. Find how many tweets each user has
twitter_df\
    .groupBy(col('user'))\
    .count()\
    .withColumnRenamed('count', 'num_tweets')\
    .orderBy(col('count').desc())\
    .show(5)

# 3. Find all the persons mentioned on tweets
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType

def generate_mentioned_user_list(text):
    return [item.lstrip('@') for item in text.split(' ') if item.startswith('@')]


twitter_df_with_mentioned = twitter_df.withColumn('users_mentioned', udf(lambda text: generate_mentioned_user_list(text), ArrayType(StringType()))(col('text')))
twitter_df_with_mentioned.show()


# 4.Count how many times each person is mentioned
from pyspark.sql.functions import explode

# explode flattens the previous step array into a column
mentioned_only_df = twitter_df_with_mentioned.select(explode(col('users_mentioned')).alias('users_mentioned'))
# the users_mentioned list contains '' also, so exclude that
mentioned_only_df = mentioned_only_df.filter(col('users_mentioned') != '')
# count of mentioned users
mentioned_only_df.groupBy('users_mentioned').count().show(truncate=False)


# 5. Find the 10 most mentioned persons
mentioned_only_df\
    .groupBy('users_mentioned')\
    .count().orderBy(col('count').desc())\
    .limit(10)\
    .show()



# 6. Find all the hashtags mentioned on a tweet
def generate_hashtags_list(text):
    return [item for item in text.split(' ') if item.startswith('#')]

twitter_df_with_hashtags = twitter_df.withColumn('hashtags', udf(lambda text: generate_hashtags_list(text), ArrayType(StringType()))(col('text')))
twitter_df_with_hashtags.show()


# 7. Count how many times each hashtag is mentioned
hashtags_only_df = twitter_df_with_hashtags.select(explode(col('hashtags')).alias('hashtags')).filter(col('hashtags') != '')

hashtags_only_df\
    .groupBy('hashtags')\
    .count()\
    .show()


# 8. Find the 10 most popular Hashtags
hashtags_only_df\
    .groupBy('hashtags')\
    .count()\
    .orderBy(col('count').desc())\
    .limit(10)\
    .show()


# 9. Find the top 5 countries which tweet the most
twitter_df\
    .groupBy('country')\
    .count()\
    .withColumnRenamed('count', 'num_tweets')\
    .orderBy(col('num_tweets').desc())\
    .limit(5)\
    .show()



