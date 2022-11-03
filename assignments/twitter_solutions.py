from pyspark.sql.functions import explode
from pyspark.sql.types import ArrayType
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, explode

postgres_username = 'postgres'
postgres_password = ''

# Load data
spark = SparkSession\
    .builder\
    .appName('twitter')\
    .config('spark.driver.extraClassPath', '/usr/lib/jvm/java-19-openjdk/lib/postgresql-42.5.0.jar')\
    .getOrCreate()

spark.read.format('json').load('data/tweets.json').schema

manual_schema = StructType([
    StructField('country', StringType(), True),
    StructField('id', StringType(), True),
    StructField('place', StringType(), True),
    StructField('text', StringType(), True),
    StructField('user', StringType(), True)
])

twitter_df = spark.read.format('json').schema(
    manual_schema).load('data/tweets.json')

twitter_df.printSchema()

twitter_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.tweets",
          properties={"user": postgres_username, "password": postgres_password})

# 1. Find all the tweets by user
user = 'Daniel Beer'
twitter_df.filter(col('user') == user).show()

twitter_df.filter(col('user') == user)\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.tweets_by_user",
          properties={"user": postgres_username, "password": postgres_password})

# 2. Find how many tweets each user has
num_tweets_df = twitter_df\
    .groupBy(col('user'))\
    .count()\
    .withColumnRenamed('count', 'num_tweets')\
    .orderBy(col('count').desc())\

num_tweets_df.show(5)

num_tweets_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.num_tweets",
          properties={"user": postgres_username, "password": postgres_password})

# 3. Find all the persons mentioned on tweets


def generate_mentioned_user_list(text):
    return [item.lstrip('@') for item in text.split(' ') if item.startswith('@')]


twitter_df_with_mentioned = twitter_df.withColumn('users_mentioned', udf(
    lambda text: generate_mentioned_user_list(text), ArrayType(StringType()))(col('text')))
twitter_df_with_mentioned.show()

twitter_df_with_mentioned\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.mentioned_users",
          properties={"user": postgres_username, "password": postgres_password})


# 4.Count how many times each person is mentioned

# explode flattens the previous step array into a column
mentioned_only_df = twitter_df_with_mentioned.select(
    explode(col('users_mentioned')).alias('users_mentioned'))
# the users_mentioned list contains '' also, so exclude that
mentioned_only_df = mentioned_only_df.filter(col('users_mentioned') != '')
# count of mentioned users
mentioned_count_df = mentioned_only_df.groupBy('users_mentioned').count()

mentioned_count_df.show(5)
mentioned_count_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.mentioned_count",
          properties={"user": postgres_username, "password": postgres_password})


# 5. Find the 10 most mentioned persons
top_10_mentioned = mentioned_only_df\
    .groupBy('users_mentioned')\
    .count().orderBy(col('count').desc())\
    .limit(10)

top_10_mentioned.show()
top_10_mentioned\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.top_mentioned",
          properties={"user": postgres_username, "password": postgres_password})


# 6. Find all the hashtags mentioned on a tweet
def generate_hashtags_list(text):
    return [item for item in text.split(' ') if item.startswith('#')]


twitter_df_with_hashtags = twitter_df.withColumn('hashtags', udf(
    lambda text: generate_hashtags_list(text), ArrayType(StringType()))(col('text')))
twitter_df_with_hashtags.show()
twitter_df_with_hashtags\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.hashtags",
          properties={"user": postgres_username, "password": postgres_password})


# 7. Count how many times each hashtag is mentioned
hashtags_only_df = twitter_df_with_hashtags.select(
    explode(col('hashtags')).alias('hashtags')).filter(col('hashtags') != '')

hashtags_count_df = hashtags_only_df\
    .groupBy('hashtags')\
    .count()

hashtags_count_df.show()
hashtags_count_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.hashtags_count",
          properties={"user": postgres_username, "password": postgres_password})


# 8. Find the 10 most popular Hashtags
top_hashtags_df = hashtags_only_df\
    .groupBy('hashtags')\
    .count()\
    .orderBy(col('count').desc())\
    .limit(10)

top_hashtags_df.show()
top_hashtags_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.top_hashtags",
          properties={"user": postgres_username, "password": postgres_password})


# 9. Find the top 5 countries which tweet the most
top_countries_df = twitter_df\
    .groupBy('country')\
    .count()\
    .withColumnRenamed('count', 'num_tweets')\
    .orderBy(col('num_tweets').desc())\
    .limit(5)

top_countries_df.show()
top_countries_df\
    .write\
    .mode('overwrite')\
    .jdbc("jdbc:postgresql:spark", "tweets.top_countries",
          properties={"user": postgres_username, "password": postgres_password})
