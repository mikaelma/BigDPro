from pyspark import SparkContext
sc = SparkContext(appName="tweets")

distFile = sc.textFile("data/geotweets.tsv")

#A 
dist_tweets=distFile.count()

#B: 
tweets = distFile.map(lambda line: line.split('\t'))
users = tweets.map(lambda tweet: tweet[6])
dist_users=users.distinct().count()

#C: 
tweets = distFile.map(lambda line: line.split('\t'))
country = tweets.map(lambda tweet: tweet[1])
dist_countries = country.distinct().count()

#D: 
tweets = distFile.map(lambda line: line.split('\t'))
place_name = tweets.map(lambda tweet: tweet[4])
dist_place = place_name.distinct().count()

#E: 
tweets = distFile.map(lambda line: line.split('\t'))
languages = tweets.map(lambda tweet: tweet[5])
dist_languages = languages.distinct().count()
#F 
tweets = distFile.map(lambda line: line.split('\t'))
latitude = tweets.map(lambda tweet: tweet[11])
min_lat = latitude.min()

#G
tweets = distFile.map(lambda line: line.split('\t'))
longitude = tweets.map(lambda tweet: tweet[12])
min_long = longitude.min()

#H
tweets = distFile.map(lambda line: line.split('\t'))
latitude = tweets.map(lambda tweet: tweet[11])
max_lat = latitude.max()

#I
tweets = distFile.map(lambda line: line.split('\t'))
longitude = tweets.map(lambda tweet: tweet[12])
max_long = longitude.max()

#J
tweets = distFile.map(lambda line: line.split('\t'))
charCount = tweets.map(lambda tweet: len(tweet[10])).collect()
dist_tweets = distFile.count()
avgCharTweet = sum(charCount)/dist_tweets

#K
tweets = distFile.map(lambda line: line.split('\t'))
wordCount = tweets.map(lambda tweet: len(tweet[10].split())).collect()
dist_tweets=distFile.count()
avgWordTweet = sum(wordCount)/dist_tweets

result = [dist_tweets, 
        dist_users, 
        dist_countries, 
        dist_place, 
        dist_languages, 
        min_lat, 
        min_long,
        max_lat,
        max_long,
        avgCharTweet,
        avgWordTweet]
sc.parallelize(result).coalesce(1).saveAsTextFile('Results/Task1')

sc.stop()