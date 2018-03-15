from pyspark import SparkContext
sc = SparkContext(appName="tweets")
distFile = sc.textFile("data/geotweets.tsv")
stopWordsInput = sc.textFile("data/stop_words.txt")


tweets = distFile.map(lambda line: line.split('\t'))
stopWords = stopWordsInput.flatMap(lambda x: x.split("\n")).map(lambda x: (x,1)).reduceByKey(lambda x,y:x+y)  

us_cities = tweets.filter(lambda tweet:(tweet[2]=='US' and tweet[3]=='city')) #Filtrerer på US og CITY

'''
#FINNER TOPP 5 BYER:
us_cities = us_cities.map(lambda tweet: (tweet[4],1))
dist_cities = us_cities.distinct().count()
count = (us_cities.reduceByKey(lambda a, b: a + b))
sortCities = count.takeOrdered(5, key=lambda x: (-1 * x[1], x[0]))
'''

#Finner distinkte ord: 
wordCount = us_cities.flatMap(lambda x: x[10].split())
wordCount = wordCount.filter(lambda x: len(x)>2)
wordCount = wordCount.map(lambda x: (x.lower(),1)).reduceByKey(lambda x,y:x+y)
#Fjerner stoppord:
wordCount = wordCount.subtractByKey(stopWords) 
#Henter 10 mest frekvente:
multipleSort = wordCount.takeOrdered(10, key=lambda x: (-1 * x[1], x[0]))


print(multipleSort)



