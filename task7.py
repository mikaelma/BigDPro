from pyspark import SparkContext
sc = SparkContext(appName="tweets")
distFile = sc.textFile("data/geotweets.tsv")
stopWordsInput = sc.textFile("data/stop_words.txt")


tweets = distFile.map(lambda line: line.split('\t'))
stopWords = stopWordsInput.flatMap(lambda x: x.split("\n")).collect()

us_cities = tweets.filter(lambda tweet:(tweet[2]=='US' and tweet[3]=='city')) #Filtrerer på US og CITY
#dist_cities = us_cities.distinct().count()


#FINNER TOPP 5 BYER:
cities = us_cities.map(lambda tweet: (tweet[4],1))
dist_cities = cities.distinct().count()
count = (cities.reduceByKey(lambda a, b: a + b))
sortCities = count.takeOrdered(5, key=lambda x: (-1 * x[1], x[0]))
result = sc.parallelize(list(sortCities))
#print(result.keys().collect())


#Finner distinkte ord: 
wordCount = us_cities.map(lambda x: (x[4], x[10].lower().split(" ")))

#Flatmapper by og ord, reduce by keys
wordCount = wordCount.flatMapValues(lambda x: x)
wordCount = wordCount.map(lambda x: (x, 1))
wordCount = wordCount.reduceByKey(lambda a, b: a+b)

resKey = result.keys().collect()

#Filtrerer på at by er i topp5 by og lengde ord er over 2
wordCount = wordCount.filter(lambda x: x[0][0] in resKey)


wordCount = wordCount.filter(lambda x: x[0][1] not in stopWords)
wordCount = wordCount.filter(lambda x: len(x[0][1])>2)

dicti =dict()
for city in resKey:
    filtering = wordCount.filter(lambda x: x[0][0] in city)
    result = filtering.takeOrdered(10, key = lambda x: (-1 * x[1], x[0]))
    for res in result:
        if(res[0][0] not in dicti):
            dicti[res[0][0]] = []
        dicti[res[0][0]].append((res[0][1], res[1]))

res = []
for entry in dicti:
    for item in dicti[entry]:
        res.append((entry, item[0], item[1]))

sc.parallelize(res)\
.map(lambda x: '\t'.join([str(word) for word in x]))\
.coalesce(1)\
.saveAsTextFile('Results/Task7')