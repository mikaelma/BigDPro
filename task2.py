from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: (tweet[1],1))
sumTweets = country.reduceByKey(lambda a, b: a + b).sortBy(lambda a: a[1], ascending=False)
print(sumTweets.collect())


result = []
sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task2')

sc.stop()