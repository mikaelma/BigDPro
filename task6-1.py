from pyspark import SparkContext
import operator
sc = SparkContext(appName="tweets")
distFile = sc.textFile("data/geotweets.tsv")
stopWordsInput = sc.textFile('data/stop_words.txt')
stopWordsInput = stopWordsInput.map(lambda line: line.split('\n')).collect()

res = []
for entry in stopWordsInput:
    res.append(entry[0])
stopWordsInput = res


def mergeDict(a,b):
    for key in b:
        if key not in a:
            a[key] = 0
        a[key]+=b[key]
    return a


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
tweets = tweets.reduceByKey(mergeDict)
words_dict = tweets.collect()[0][1]
for word in stopWordsInput:
    words_dict[word] = 0

sorted_x = sorted(words_dict.items(), key=operator.itemgetter(1),reverse=True)   
count = 0
for key in sorted_x:
    print(key)
    count+=1
    if count>10:
        break
