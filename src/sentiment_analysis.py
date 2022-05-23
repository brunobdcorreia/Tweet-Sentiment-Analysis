from typing import Text
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from src.preprocessing import preprocess
from textblob import TextBlob


# PySpark UDF to detect polarity on a piece of text.
@udf(returnType=FloatType())
def detect_polarity(doc):
    return TextBlob(doc).sentiment[0]


# PySpark UDF to detect subjectivity on a piece of text.
@udf(returnType=FloatType())
def detect_subjectivity(doc):
    return TextBlob(doc).sentiment[1]


def classify_text(words):
    words = words.withColumn('polarity', detect_polarity('word'))
    words = words.withColumn('subjectivity', detect_subjectivity('word'))
    return words


if __name__ == '__main__':
    spark = SparkSession.builder.appName('SentimentAnalysis').getOrCreate()
    lines = spark.readStream.format('socket').option('host', '0.0.0.0').option(
        'port', 5555).load()
    words = preprocess(lines)
    words = classify_text(words)
    words.repartition(1)
    query = words.writeStream.queryName('all_tweets').format(
        'json').outputMode('append').option('path', './spark-output').option(
            'checkpointLocation',
            './check').trigger(processingTime='5 seconds').start()
    query.awaitTermination()