from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: (tweet[1], 1))
dist_countries = country.distinct().count()
sumTweets = country.reduceByKey(lambda a, b: a + b)
multipleSort = sumTweets.takeOrdered(
    dist_countries, key=lambda x: (-1 * x[1], x[0]))


sc.parallelize(multipleSort)\
.map(lambda x: '\t'.join([str(word) for word in x]))\
.coalesce(1)\
.saveAsTextFile('Results/Task2')

sc.stop()
