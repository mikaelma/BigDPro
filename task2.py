from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: tweet[1])
dist_countries = country.distinct().count()

tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: (tweet[1],1))
sumTweets = country.reduceByKey(lambda a, b: a + b)
multipleSort = sumTweets.takeOrdered(dist_countries, key=lambda x: (-1 * x[1], x[0]))

result = [multipleSort]
sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task2')

sc.stop()