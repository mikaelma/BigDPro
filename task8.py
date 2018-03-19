from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *

sc = SparkContext(appName="tweets")
spark = SparkSession(sc).builder.getOrCreate()
distFile = sc.textFile("data/geotweets.tsv")

tweets = distFile.map(lambda line: line.split('\t'))
tweets = tweets.map(lambda tweet:(tweet[6],tweet[1],tweet[4],tweet[5],float(tweet[12]),float(tweet[11]),tweet[10]))

#Declare schema to be used for tweet dataframe, need to specify types
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
#Create a view of the data, so that the tweets table can be accessed by the spark sql context
schemaTweets.createOrReplaceTempView("tweets")
distinct_tweets = schemaTweets.distinct().count()


#Extract features known to be integer values
userDf = spark.sql('SELECT username as username from tweets').distinct()
distinct_users = userDf.count()
distinct_countries = spark.sql('SELECT DISTINCT country_name from tweets').count()
distinct_places = spark.sql('SELECT DISTINCT place_name from tweets').count()
distinct_langue = spark.sql('SELECT DISTINCT language from tweets').count()
#Add data and create dataframe
l = [(distinct_tweets,distinct_users,distinct_countries,distinct_places,distinct_langue)]
spark.createDataFrame(l, ['tweets','users','country','place','language']).show()

#Extract features known to be floating points
min_row = spark.sql('SELECT MIN(longitude) AS minlong, MIN(latitude) AS minlat from tweets').collect()
minlong = min_row[0]['minlong']
minlat = min_row[0]['minlat']
max_row = spark.sql('SELECT MAX(longitude) AS maxlong, MAX(latitude) AS maxlat from tweets').collect()
maxlong = max_row[0]['maxlong']
maxlat = max_row[0]['maxlat']
#Add data and create dataframe
l = [(minlong,minlat,maxlong,maxlat)]
spark.createDataFrame(l,['Minimum longitude','Minimum latitude','Maximum longitude','Maximum latitude']).show()
