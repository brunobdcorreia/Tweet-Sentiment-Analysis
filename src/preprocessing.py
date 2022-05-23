from pyspark.sql import functions as sf
from pyspark.sql.functions import *
from pyspark.sql.types import *


def preprocess(lines):
    # Split the tweets at 'tweet_end' and remove empty rows from our DF
    words = lines.select(
        explode(split(lines.value, 'tweet_end')).alias('word'))
    words = words.na.replace('', None)
    words = words.na.drop()
    # Remove any links from the tweet
    words = words.withColumn('word', sf.regexp_replace('word', r'http\S+', ''))
    # Remove mentions
    words = words.withColumn('word', sf.regexp_replace('word', r'@\w+', ''))
    # Remove hashtags
    words = words.withColumn('word', sf.regexp_replace('word', '#', ''))
    # Remove RT (which represents tweets that are actually retweets)
    words = words.withColumn('word', sf.regexp_replace('word', 'RT', ''))
    # Remove the character ':'
    words = words.withColumn('word', sf.regexp_replace('word', ':', ''))
    return words