import json
import time

from pprint import pprint
from os import listdir, path as p
from collections import defaultdict

'''
	Filter on attributes: 
		tweet location - country code au and preferably city name.
		geo - longitude, latitude. NOMINATIM.
		place - country code au and city name.

	Filter conditions:
		If geo/place suggests city in AU. -> category 1 (primary)
		If tweet content contains a city in AU. -> category 2 (NEED TEST)
			RESULT foreign language tweets, news tweets involve Australia
		If tweet content contains country_code au and only (meaning not city or state) -> category 3 (NEED TEST)
			RESULT returns a huge number of French tweets ...

	Output file structure
		|-  date_category_3
		|-  name of city
			|-  date_category1/2

	geo attribute sample:
		geo: {'country_code': 'us', 'state': 'Florida', 'county': 'Miami-Dade County', 'city': 'Miami'}

'''

folder_name = "/shared/COVID19Tweet"

def filter(tweet):
	'''
		Change this function to change filter conditions.
	'''
	if tweet['geo'] and tweet['geo']['country_code']=='au':
		if 'city' in tweet['geo'].values(): tweets[tweet['geo']['city']].append(tweet['id'])


def main():
	#load file
	tweets1 = defaultdict(list) #category1
	tweets2 = defaultdict(list) #category2
	tweets3 = [] #category3

	foldername = "COVID19Tweet"
	file_of_interest = listdir(foldername)

	for filebydate in file_of_interest:
		print()
		print(filebydate)

		for i, tweet in enumerate(open(p.join(foldername, filebydate))):
			if i%100000==0: print(i) #count
			
			tweet = json.loads(tweet)
			t_id = tweet['tweet_id']

			if tweet['geo'] and tweet['geo']['country_code']=='au':
				#category 1
				if 'city' in tweet['geo'].keys(): 
					city = tweet['geo']['city']
					tweets1[city].append(t_id)
				else: 
					# pprint(tweet)
					tweets1['nation-wide'].append(t_id)

			else:
				for place in tweet['tweet_locations']:
					if place['country_code'] == "au":
						
						#category 3
						if len(place.keys())<2:
							tweets3.append(t_id)

						#category 2
						if "city" in place.keys():
							tweets2[place['city']].append(t_id)

		json.dump(tweets1, open("cat1_aus_%s.json" %(filebydate[4:-5]), 'w'))
		json.dump(tweets2, open("cat2_aus_%s.json" %(filebydate[4:-5]), 'w'))
		json.dump(tweets3, open("cat3_aus_%s.json" %(filebydate[4:-5]), 'w'))

	print("Done")

if __name__ == '__main__':
	main()