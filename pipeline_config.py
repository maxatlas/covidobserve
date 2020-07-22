import json
from pprint import pprint

filter_entity_dict={
	"delim": "\n----*****----\n",
	"replacement": {
		"AU":"Australia",
		"Au":"Australia",
		"Aus":"Australia",
		"aus":"Australia",
		"au":"Australia",
		"Australia ":"Australia",
		"Australia's":"Australia",
		"AUSTRALIA":"Australia",
		"Aust":"Australia",
		"Australia🇦🇺":"Australia",
		"NZ":"New Zealand",
		"Bo Jo":"Boris Johnson",
		"BoJo":"Boris Johnson",
		"ScottMorrison":"Scott Morrison",
		"scomo":"Scott Morrison",
		"@ScottMorrisonMP":"Scott Morrison",
		"coronavirus":"Coronavirus",
		"CORONAVIRUS":"Coronavirus",
		"CoronaVirus":"Coronavirus",
		"Cov-19":"Coronavirus",
		"cov-19":"Coronavirus",
		"COV-19":"Coronavirus",
		"covid":"Coronavirus",
		"Covid":"Coronavirus",
		"COVID":"Coronavirus",
		"Covid19":"Coronavirus",
		"covid19":"Coronavirus",
		"COVID19":"Coronavirus",
		"Covid 19":"Coronavirus",
		"covid 19":"Coronavirus",
		"COVID 19":"Coronavirus",
		"Covid-19":"Coronavirus",
		"covid-19":"Coronavirus",
		"COVID-19":"Coronavirus",
		"Covid 19/Coronavirus":"Coronavirus",
		"Corona Virus":"Coronavirus",
		"corona virus":"Coronavirus",
		"CORONA VIRUS":"Coronavirus",
		"Coronavirus Australia":"Coronavirus",
		"Covid_19":"Coronavirus",
		"covid_19":"Coronavirus",
		"COVID_19":"Coronavirus",
		"COVID - 19":"Coronavirus",
		"covid - 19":"Coronavirus",
		"Covid - 19":"Coronavirus",
		"COVID2019":"Coronavirus",
		"covid2019":"Coronavirus",
		"Covid2019":"Coronavirus",
		"@tomhanks":"TomHanks",
		"tomhanks":"TomHanks",
		"Tom Hanks":"TomHanks",
		"Tom Hanx":"TomHanks",
		"USA":"U.S",
		"U.S.A":"U.S",
		"U.S.":"U.S",
		"UnitedStates":"U.S",
		"United States":"U.S",
		"united states":"U.S",
		"America":"U.S",
		"America🇺🇸":"U.S",
		"HongKong":"Hong Kong",
		"HK":"Hong Kong",
		"Hong Kong SAR":"Hong Kong",
	},

	"exclude_types": ["EST TIME", "DATE", "CARDINAL", "LAW", "ORDINAL", "TIME", "PERCENT", "QUANTITY", "MONEY"],
	"include_types": ["PERSON", "ORG", "GPE", "FAC"]
	}

folder_names={
	1:"1.Raw",
	2:"2.Tweet Ids",
	3:"3.Tweet Texts",
	4:"4.NERs",
	5:"5.Graphs",
	6:"6.Peaking Entities",
	7:"7.N&NPs",
	8:"8.KeyGraph",
	9:"9.Word Clouds"
}

def get_folder_names():
	return folder_names

def filter_entity():
	return filter_entity_dict

def filter_by_loc(tweet_meta, ratio=0.6, depth=0, depth_gt=0, depth_lt=0):
	if tweet_meta["tweet_locations"] and not (tweet_meta['user_location'] or tweet_meta['place'] or tweet_meta['geo']):
		au_locs = [filter_by_loc_depth(loc, depth=depth, depth_lt=depth_lt, depth_gt=depth_gt) for loc in tweet_meta["tweet_locations"]]
		return sum(au_locs)/len(tweet_meta["tweet_locations"])>ratio

def filter_by_loc_depth(loc, depth=0, depth_gt=0, depth_lt=0):
	'''
		tweet_locations: [loc]
		loc: {"country_code": ..., "city":...}

		depth:
			=1 country
			=2 country, state
		depth_gt:
			=1 must at least contain country
			=2 must at least contain state

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
		else full_text = retweet text + orginal tweet
		return ID, full_text
		
	'''
	return tweet['lang']=='en'

def get_full_text(tweet):
	'''
		Return ID, full_text
		Same as filter_en except not looking at tweet['lang']

	'''
	if not tweet.get("retweeted_status") or tweet.get("retweeted_status").get("retweeted"): return  tweet['full_text']
	else:
		try:
			return tweet['full_text'][:tweet['full_text'].find("@"+tweet['entities']['user_mentions'][0]['screen_name'])]+tweet["retweeted_status"]["full_text"]
		except Exception: return tweet['full_text']

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

	by_au, by_loc = [], []
	for t in open('1.Get Tweet Ids/en_geo_2020-04-13.json'):
		tweet_meta = json.loads(t)
		# if filter_by_au(tweet_meta): print("\n"+get_tweet_by_id(tweet_meta['tweet_id']))
		if filter_by_au(tweet_meta): by_au.append(True)
		if filter_by_loc(tweet_meta): by_loc.append(True)
	
	print("by au: %i" %sum(by_au))
	print("by loc: %i" %sum(by_loc))