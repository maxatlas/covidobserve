import json


def filter_by_loc(tweet_meta, filter_key):
	'''
		A dictionary
	'''
	for place in tweet_meta['tweet_locations']:
		if place['country_code'] == "au":
			#category 3
			if len(place.keys())<2: return tweet_meta['tweet_id']

def filter_en(tweet):
	'''
		If the tweet is in English. Return full_text attribute or retweeted full_text.
	'''
	if tweet['lang']=='en': out = tweet['retweeted_status']['full_text'] if 'retweeted_status' in tweet.keys() else tweet['full_text']
	return [out]

def filter_by_city(tweet_meta, filter_keys):
	for filter_key in filter_keys:
		if filter_key in tweet_meta.keys() and tweet_meta[filter_key] and tweet_meta[filter_key]['country_code']=='au':
			if 'city' in tweet_meta[filter_key].keys(): return tweet_meta[filter_key]['city']

def filter_by_au(tweet, filter_keys):
	for filter_key in filter_keys:
		if filter_key in tweet_meta.keys() and tweet_meta[filter_key] and tweet_meta[filter_key]['country_code']=='au':
			if 'city' not in tweet_meta[filter_key].keys(): return True