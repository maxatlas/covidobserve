import json
import time

from pprint import pprint
from os import listdir, path as p
from collections import defaultdict

from preprocess_config import filter_by_city, filter_by_au
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


def main(filterfunc, keyword, file_path, output_folder="2.Filter by Location"):
	'''
	filter
	'''
	#load file
	tweet_ids = defaultdict(list)
	file_name = file_path.split("/")[-1]
	print("\n"+file_path)

	for i, tweet_meta in enumerate(open(file_path)):
		if i%100000==0: print(i) #count
		
		tweet_meta = json.loads(tweet_meta)
		t_id = tweet_meta['tweet_id']

		filtered = filterfunc(tweet_meta, keyword)
		if filtered is True: tweet_ids["nation-wide"].append(t_id)
		elif filtered: tweet_ids[filtered].append(t_id)

	json.dump(tweet_ids, open("%s.json" %(p.join(output_folder, file_name[:-5])), 'w'))
	print("File saved.")


	print("Done")

if __name__ == '__main__':
	main(filter_by_city, ["place", "coordinates"], file_path="RAW/geo_place.json", )
