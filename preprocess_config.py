


def filter_loc(tweet_meta):
	'''
		A dictionary
	'''
	tweet_id = tweet_meta['tweet_id']
	for place in tweet_meta['tweet_locations']:
		if place['country_code'] == "au":
			#category 3
			if len(place.keys())<2: return tweet_id

def filter_en(tweet):
	'''
		If the tweet is in English. Return full_text attribute or retweeted full_text.
	'''
	if tweet['lang']=='en': out = tweet['retweeted_status']['full_text'] if 'retweeted_status' in tweet.keys() else tweet['full_text']
	return [out]