from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
us_cities = tweets.filter(lambda tweet:(tweet[2]=='US' and tweet[3]=='city'))
count_city = us_cities.map(lambda entry:(entry[4],1))
count_city = count_city.reduceByKey(lambda accum,item:(accum+item))
count_city = count_city.takeOrdered(count_city.count(),key=lambda entry:(-1*entry[1],entry[0]))


sc.parallelize(count_city)\
.map(lambda x: '\t'.join([str(word) for word in x]))\
.coalesce(1)\
.saveAsTextFile('Results/Task5')