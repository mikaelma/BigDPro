from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import operator
sc = SparkContext(appName="tweets")
spark = SparkSession(sc).builder.getOrCreate()
distFile = sc.textFile("data/geotweets.tsv")
stopWordsInput = sc.textFile('data/stop_words.txt')
tweets = distFile.map(lambda line: line.split('\t'))
tweets = tweets.map(lambda tweet:(tweet[6],tweet[1],tweet[4],tweet[5],float(tweet[12]),float(tweet[11]),tweet[10]))
print(tweets.take(50))

fields = [
    StructField('username',StringType(),True),
    StructField('country_name',StringType(),True),
    StructField('place_name',StringType(),True),
    StructField('language',StringType(),True),
    StructField('longitude',FloatType(),True),
    StructField('latitude',FloatType(),True),
    StructField('text',StringType(),True)
    ]
schema = StructType(fields)

schemaTweets = spark.createDataFrame(tweets,schema)
schemaTweets.show()