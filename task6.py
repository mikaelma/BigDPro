from pyspark import SparkContext
sc = SparkContext(appName="tweets")
distFile = sc.textFile("data/geotweets.tsv")
stopWordsInput = sc.textFile("data/stop_words.txt")


tweets = distFile.map(lambda line: line.split('\t'))
stopWords = stopWordsInput.flatMap(lambda x: x.split("\n")).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)  

#Filtrerer på US og CITY
us_cities = tweets.filter(lambda tweet:(tweet[2]=='US' and tweet[3]=='city')) 

#Finner distinkte ord: 
wordCount = us_cities.flatMap(lambda x: x[10].split())
wordCount = wordCount.filter(lambda x: len(x)>2)
wordCount = wordCount.map(lambda x: (x.lower(),1)).reduceByKey(lambda x,y:x+y)

#Fjerner stoppord:
wordCount = wordCount.subtractByKey(stopWords) 

#Henter 10 mest frekvente:
multipleSort = wordCount.takeOrdered(10, key=lambda x: (-1 * x[1], x[0]))

sc.parallelize(multipleSort)\
.map(lambda x: '\t'.join([str(word) for word in x]))\
.coalesce(1)\
.saveAsTextFile('Results/Task6')