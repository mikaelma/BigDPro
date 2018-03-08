from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
countryLongLat = tweets.map(lambda tweet: (tweet[1], (tweet[11], tweet[12]))).groupByKey()
filtered = countryLongLat.filter(lambda a: len(a[1])>10)




#sumTweets = country.reduceByKey(lambda a, b: a + b)

print(filtered.collect())

#result = []
#sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task2')

sc.stop()