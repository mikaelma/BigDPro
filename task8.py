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
schemaTweets.createOrReplaceTempView("tweets")
distinct_tweets = schemaTweets.distinct().count()
"""
userDf = spark.sql('SELECT username as username from tweets').distinct()
distinct_users = userDf.count()
distinct_countries = spark.sql('SELECT DISTINCT country_name from tweets').count()
distinct_places = spark.sql('SELECT DISTINCT place_name from tweets').count()
distinct_langue = spark.sql('SELECT DISTINCT language from tweets').count()
min_row = spark.sql('SELECT MIN(longitude) AS minlong, MIN(latitude) AS minlat from tweets').collect()
print(min_row[0]['minlong'])
minlong = min_row[0]['minlong']
minlat = min_row[0]['minlat']
max_row = spark.sql('SELECT MAX(longitude) AS maxlong, MAX(latitude) AS maxlat from tweets').collect()
maxlong = max_row[0]['maxlong']
maxlat = max_row[0]['maxlat']
l = [(distinct_tweets,distinct_users,distinct_countries,distinct_places,distinct_langue,minlong,minlat,maxlong,maxlat)]
c = [
    StructField('Tweets',IntegerType(),True),
    StructField('Users',IntegerType(),True),
    StructField('Countries',IntegerType(),True),
    StructField('Places',IntegerType(),True),
    StructField('Languages',IntegerType(),True),
    StructField('Min. Longitude',FloatType(),True),
    StructField('Min. Latitude',FloatType(),True),
    StructField('Max. Longitude',FloatType(),True),
    StructField('Max. Latitude',FloatType(),True)
    ]
res_schema = StructType(c)
res_rdd = sc.parallelize(l)
spark.createDataFrame(l,c).show()
"""
l = [(distinct_tweets)]
spark.createDataFrame(l, ['tweets']).show()