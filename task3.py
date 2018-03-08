from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#Finding distinct countries
tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: tweet[1])
dist_countries = country.distinct().count()

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: (tweet[1], 1))
sumTweets = country.reduceByKey(lambda a, b: a + b)
filtered = sumTweets.filter(lambda a: a[1]>10)
print(filtered.collect())

#result = []
#sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task2')

sc.stop()