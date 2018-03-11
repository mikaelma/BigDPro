from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
us_cities = tweets.filter(lambda tweet:(tweet[2]=='US' and tweet[3]=='city'))
count_city = us_cities.map(lambda entry:(entry[4],1))
count_city = count_city.reduceByKey(lambda accum,item:(accum+item))
#multipleSort = sumTweets.takeOrdered(dist_countries, key=lambda x: (-1 * x[1], x[0]))
count_city = count_city.takeOrdered(count_city.count(),key=lambda entry:(-1*entry[1],entry[0]))
for item in count_city:
    print(item)