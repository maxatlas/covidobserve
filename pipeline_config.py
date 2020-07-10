import json
from pprint import pprint

filter_entity_dict={
	"delim": "\n----*****----\n",
	"replacement": {
		"AU" : "Australia",
		"NZ" : "New Zealand"
	},

	"exclude_types": ["EST TIME", "DATE", "CARDINAL", "LAW", "ORDINAL", "TIME", "PERCENT", "QUANTITY", "MONEY"],
	"include_types": ["PERSON", "ORG", "GPE", "FAC"]
	}

folder_names={
	1:"1.Get Tweet Ids",
	2:"2.Filter by Location",
	3:"4.Get Tweets",
	4:"5.Get NER Entities",
	5:"6.Graphs"
}

def get_folder_names():
	return folder_names

def filter_entity():
	return filter_entity_dict

def filter_by_loc(tweet_meta):
	if tweet_meta["tweet_locations"] and not (tweet_meta['user_location'] or tweet_meta['place'] or tweet_meta['geo']):
		au_locs = [filter_by_loc_depth(loc, depth=1) for loc in tweet_meta["tweet_locations"]]
		return sum(au_locs)/len(tweet_meta["tweet_locations"])>0.6

def filter_by_loc_depth(loc, depth=0, depth_gt=0, depth_lt=0):
	'''
		tweet_locations: [loc]
		loc: {"country_code": ..., "city":...}

	'''
	assert not(depth and depth_gt and depth_lt==1) , "depth_lt needs to be 2 at least."

	if not (depth or depth_lt or depth_gt): return loc["country_code"]=="au"
	elif loc['country_code'] == "au": 
		if depth: return len(loc)==depth
		if depth_gt: return len(loc)>depth_gt
		if depth_lt: return len(loc)<depth_lt
	return False

def filter_en(tweet):
	'''
		If the tweet is in English. Return full_text attribute if it's not a RT, or, return the retweeted content.
		return ID, full_text
	'''
	if tweet['lang']=='en': 
		if not tweet.get("retweeted_status") or tweet.get("retweeted_status").get("retweeted"): out = (tweet['id'], tweet['full_text'])
		else:
			try:
				out = (tweet['id'], tweet['full_text'][:tweet['full_text'].find("@"+tweet['entities']['user_mentions'][0]['screen_name'])]+tweet["retweeted_status"]["full_text"])
			except Exception: out = (tweet['id'], tweet['full_text'])
	else: out = (None, None)
	return out

def get_full_text(tweet):
	'''
		Return ID, full_text
	'''
	if not tweet.get("retweeted_status") or tweet.get("retweeted_status").get("retweeted"): out = (tweet['id'], tweet['full_text'])
	else:
		try:
			out = (tweet['id'], tweet['full_text'][:tweet['full_text'].find("@"+tweet['entities']['user_mentions'][0]['screen_name'])]+tweet["retweeted_status"]["full_text"])
		except Exception: out = (tweet['id'], tweet['full_text'])
	return out

def filter_by_city(tweet_meta, filter_keys):
	for filter_key in filter_keys:
		if filter_key in tweet_meta.keys() and tweet_meta[filter_key] and tweet_meta[filter_key]['country_code']=='au':
			if 'city' in tweet_meta[filter_key].keys(): return tweet_meta[filter_key]['city']

def filter_by_au(tweet_meta, filter_keys=["place", "geo", "user_location"], depth=0, depth_gt=0, depth_lt=0):
	assert not (depth and depth_gt and depth_lt==1), "depth_lt must be 2 at least."

	'''
		filter_key OR filter_key ...
		depth:
			=1 country
			=2 country, state
		depth_gt:
			=1 must at least contain country
			=2 must at least contain state
	'''
	for filter_key in filter_keys:
		if not (depth and depth_gt and depth_lt): return tweet_meta[filter_key].get("country_code")=="au"
		else:
			if tweet_meta[filter_key].get('country_code')=='au': 
				if depth: return len(tweet_meta[filter_key])==depth
				if depth_gt: return len(tweet_meta[filter_key])>depth_gt
				if depth_lt: return len(tweet_meta[filter_key])<depth_lt

if __name__ == '__main__':
	from utils import get_tweet_by_id

	for t in open('1.Get Tweet Ids/en_geo_2020-03-29.json'):
		tweet_meta = json.loads(t)
		# if filter_by_au(tweet_meta): print("\n"+get_tweet_by_id(tweet_meta['tweet_id']))
		if filter_by_loc(json.loads(t)): 
			print(json.loads(t))
			print(get_tweet_by_id(tweet_meta['tweet_id'])+"\n")