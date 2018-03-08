from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")




result = []
sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task2')
sc.stop()