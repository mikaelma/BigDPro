from pyspark import SparkContext
from math import floor
import operator
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#Create a dict whose keys are hours and values are tweets druing the respective hour. 
def hour_counter(entry):
    hours = dict()
    for item in entry[1]:
        if item not in hours:
            hours[item] = 0
        hours[item]+=1
    return (entry[0],hours)

#Mapping 
tweets = distFile.map(lambda line: line.split('\t'))
#Map by country, time of tweet and  timezone offset
time = tweets.map(lambda tweet:(tweet[1],(int(tweet[0]),int(tweet[8]))))
#Add the timezone offset and time of tweet in order to aquire the local time of tweet
time_normalized  = time.map(lambda temporality:(temporality[0],(temporality[1][0]+temporality[1][1]*1000)))
#Figure out the amount of hours in the unix timestamp, then modulo this by 24 to get the hour of tweet
hours = time_normalized.map(lambda entry:(entry[0],floor(entry[1]/3600000)%24))
<<<<<<< HEAD
#Group by keys, creating iterable list of tweet hours
counts_collected = hours.groupByKey()
countlist = counts_collected.map(lambda entry:hour_counter(entry))
#The max() call returns the keys of max values in decending order, looks sort of messy when used like below 
=======
counts = hours.map(lambda entry:((entry[0],entry[1])))
zeroValue = ('',dict)
counts_collected = counts.groupByKey()
countlist = counts_collected.map(lambda entry:darkMagic(entry))

>>>>>>> 5f75d38b07d524d16e253ecdffc1af7c8ae61d1f
max_traffic = countlist.map(lambda entry:(entry[0],max(entry[1].items(), key=operator.itemgetter(1))[0],entry[1][max(entry[1].items(),key=operator.itemgetter(1))[0]]))
print(max_traffic.collect())

sc.parallelize(max_traffic.collect())\
.map(lambda entry:'\t'.join(str(word) for word in entry)).coalesce(1).saveAsTextFile('Results/Task4')


sc.parallelize(max_traffic)\
.map(lambda x: '\t'.join([str(word) for word in x]))\
.coalesce(1)\
.saveAsTextFile('Results/Task4')


