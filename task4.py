from pyspark import SparkContext
from math import floor
import operator
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

def darkMagic(entry):
    hours = dict()
    for item in entry[1]:
        if item not in hours:
            hours[item] = 0
        hours[item]+=1
    return (entry[0],hours)

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
countryLongLat = tweets.map(lambda tweet: (tweet[1], (float(tweet[11]), float(tweet[12]),1)))
time = tweets.map(lambda tweet:(tweet[1],(int(tweet[0]),int(tweet[8]))))
time_normalized  = time.map(lambda temporality:(temporality[0],(temporality[1][0]+temporality[1][1]*1000)))
hours = time_normalized.map(lambda entry:(entry[0],floor(entry[1]/3600000)%24))
counts = hours.map(lambda entry:((entry[0],entry[1])))
zeroValue = ('',dict)
counts_collected = counts.groupByKey()
countlist = counts_collected.map(lambda entry:darkMagic(entry))
max_traffic = countlist.map(lambda entry:(entry[0],max(entry[1].items(), key=operator.itemgetter(1))[0]))




