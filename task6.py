from pyspark import SparkContext
sc = SparkContext(appName="tweets")
distFile = sc.textFile("data/geotweets.tsv")


#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
tweets = tweets.filter(lambda tweet:(tweet[2]=='US'))

def longerLower(entries):
    res = dict()
    entries = entries.split()
    for entry in entries:
        if len(entry)>2 and entry.islower():
            if entry not in res:
                res[entry] = 0
            res[entry]+=1
    return res



tweets = tweets.map(lambda entry:(entry[2],longerLower(entry[10])))
tweets = tweets.filter(lambda entry:(len(list(entry[1].keys()))>0))

#tweets = tweets.reduceByKey(lambda a,b:(mergeDictionaries(a,b)))
print(tweets.take(500))

