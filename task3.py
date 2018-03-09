from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
countryLongLat = tweets.map(lambda tweet: (tweet[1], (float(tweet[11]), float(tweet[12]),1)))
filtered = countryLongLat.filter(lambda a: len(a[1])>10)
maximals = countryLongLat.reduceByKey(lambda accum,val:(accum[0]+val[0],accum[1]+val[1],accum[2]+val[2]))
filtered_maximals = maximals.filter(lambda values:(values[1][2]>10))
centroids = filtered_maximals.map(lambda source:(source[0],(source[1][0]/source[1][2],source[1][1]/source[1][2])))
print(centroids.collect())




#sumTweets = country.reduceByKey(lambda a, b: a + b)

print(filtered.take(5))

#result = []
#sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task2')

sc.stop()