'''
	currently inlcuded:
		cat3 file -> ids
'''
import json
import re

from pprint import pprint
from os import listdir, path as p
from collections import defaultdict

def id2tweets(from_path="3.Get English Tweet Ids/cat3", to_path="4.Get Tweets"):
	
	files = listdir(path)
	
	for from_file in files:
		print(from_file)
		
		data = json.load(open(p.join(from_path, from_file)))
		date = from_file.split("_")[-1][:-5]

		filetype="txt"
		try: to_file = open('%s/%s.%s' %(to_path, date, filetype), "x")
		except FileExistsError as e: print(e)
		to_file = open('%s/%s.%s' %(to_path, date, filetype), "w")
		
		file_i, i = 0, 0
		for tweet_id in data: 
			if i//100000 >1:
				i=0
				file_i+=1
				to_file.close()
				to_file = open('%s/%s_%i.%s' %(to_path, date, file_i, filetype), "x")
			to_file.write(tweet_id+"\n")
			i+=1 
		to_file.close()

def tweets2text(from_path="4.Get Tweets", to_path="5.Clean Tweets/sentences", file=None):
	'''
		output is a list of full_text.
	'''
	files = [file] if file else listdir(from_path)

	for file in files[12:]:
		print(file)
		tweets = open(p.join(from_path, file))
		tweets = tweets.readlines()
		print("Tweets read.")
		texts = []
		for tweet in tweets:
			tweet = json.loads(tweet)
			if tweet['lang']=='en': texts.append(tweet['retweeted_status']['full_text'] if 'retweeted_status' in tweet.keys() else tweet['full_text'])

		json.dump(texts, open("%s/%s" %(to_path, file), "w"))
		print("Done for %s. Total tweets: %i" %(file, len(texts)))
		

if __name__ == '__main__':
	# id2tweets()
	tweets2text()

'''
twarc hydrate nation-wide_ids/2020-03-31.txt > nation-wide_tweets/2020-03-31.json
twarc hydrate nation-wide_ids/2020-04-01.txt > nation-wide_tweets/2020-04-01.json
twarc hydrate nation-wide_ids/2020-04-02.txt > nation-wide_tweets/2020-04-02.json
twarc hydrate nation-wide_ids/2020-04-03.txt > nation-wide_tweets/2020-04-03.json
twarc hydrate nation-wide_ids/2020-04-04.txt > nation-wide_tweets/2020-04-04.json
twarc hydrate nation-wide_ids/2020-04-05.txt > nation-wide_tweets/2020-04-05.json
twarc hydrate nation-wide_ids/2020-04-06.txt > nation-wide_tweets/2020-04-06.json
twarc hydrate nation-wide_ids/2020-04-07.txt > nation-wide_tweets/2020-04-07.json
twarc hydrate nation-wide_ids/2020-04-08.txt > nation-wide_tweets/2020-04-08.json
twarc hydrate nation-wide_ids/2020-04-09.txt > nation-wide_tweets/2020-04-09.json
twarc hydrate nation-wide_ids/2020-04-09_1.txt > nation-wide_tweets/2020-04-09_1.json
twarc hydrate nation-wide_ids/2020-04-09_2.txt > nation-wide_tweets/2020-04-09_2.json
twarc hydrate nation-wide_ids/2020-04-09_3.txt > nation-wide_tweets/2020-04-09_3.json
twarc hydrate nation-wide_ids/2020-04-09_4.txt > nation-wide_tweets/2020-04-09_4.json
twarc hydrate nation-wide_ids/2020-04-10.txt > nation-wide_tweets/2020-04-10.json
twarc hydrate nation-wide_ids/2020-04-10_1.txt > nation-wide_tweets/2020-04-10_1.json
twarc hydrate nation-wide_ids/2020-04-10_2.txt > nation-wide_tweets/2020-04-10_2.json
twarc hydrate nation-wide_ids/2020-04-10_3.txt > nation-wide_tweets/2020-04-10_3.json
twarc hydrate nation-wide_ids/2020-04-10_4.txt > nation-wide_tweets/2020-04-10_4.json
twarc hydrate nation-wide_ids/2020-04-11.txt > nation-wide_tweets/2020-04-11.json
twarc hydrate nation-wide_ids/2020-04-11_1.txt > nation-wide_tweets/2020-04-11_1.json
twarc hydrate nation-wide_ids/2020-04-11_2.txt > nation-wide_tweets/2020-04-11_2.json
twarc hydrate nation-wide_ids/2020-04-11_3.txt > nation-wide_tweets/2020-04-11_3.json
twarc hydrate nation-wide_ids/2020-04-11_4.txt > nation-wide_tweets/2020-04-11_4.json
'''
