import re
import time
import stanza
import json

from os import listdir, path as p
from pprint import pprint

def alter_token(token):
	token = re.sub("[\u00a0-\uffff]", "", token)
	token = re.sub("&amp", "&", token)

	token = token[3:] if token.startswith("RT ") else token
	token = token[1:] if token.startswith("#") else token
	token = token[:-1] if token.endswith(" ") else token
	token = token[:-1] if token.endswith("&") else token
	token = token[:-1] if token.endswith(" ") else token
	token = token[:-2] if token.endswith("'s") else token

	return token
		
def get_tweet_by_id(tweet_id):
	from os import environ as e
	from twarc import Twarc

	out = ""
	twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])
	tweet = list(twarc.hydrate([tweet_id]))
	if tweet:
		tweet=tweet[0]
		if not tweet.get("retweeted_status") or tweet.get("retweeted_status").get("retweeted"): out =  tweet['full_text']
		else:
			try:
				out = tweet['full_text'][:tweet['full_text'].find("@"+tweet['entities']['user_mentions'][0]['screen_name'])]+tweet["retweeted_status"]["full_text"]
			except Exception: out = tweet['full_text']

	return out

def is_person(e):
	qualified_types = ["PERSON", "FAC"]
	return e.type in qualified_types 

def get_name(e):
	'''
		example:
		
		realDonaldTrump -> Donald Trump
		realdonaldtrump -> realdonaldtrump
	'''
	if is_person(e):
		out = re.findall("[A-Z][a-z]+", e.text)# *: Scott Morrison P M; +: Scott Morrison
		out = " ".join(out) if out else e.text

		return out
	return e.text

def ner_sent(sent, report=False):
	if report: print("Start sending NER...")
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner')

	start = time.time()
	out = pipe(sent)
	end = time.time()
	if report: print("Batch sent takes %f seconds" %(end-start))

	return out

def is_topic(token):
	return token.startswith("#")

def get_topic(e):
	'''
		example:

		Covid19Australia -> Covid19 Australia
		covid19australia -> covid19australia
	'''
	assert is_topic(e), "Input needs to start with '#'. E.g #COVID-19."
	e = e[1:]

	if e:
		out = re.findall("[A-Z][a-z0-9]+", e)
		out = out if out else [e]
	else: out = []

	return out

def combine_files(folder, keyword, from_i=None, to_i=None):
	'''
		use under the save folder
	'''
	l=[]
	files = [file for file in listdir(folder) if file.startswith(keyword)]
	files.sort()
	print(files)
	for file in files[from_i: to_i]: l+=json.load(open(p.join(folder,file)))
	print(len(l))
	new_file = "%s_all.json" % keyword if not (from_i or to_i) else "%s_%i_to_%i"%(keyword, from_i, to_i)
	json.dump(l, open(p.join(folder, new_file), "w"))

def get_twarc_instance():
	from os import environ as e
	from twarc import Twarc
	
	twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])
	return twarc

def get_loc(tweet_id, loc_list):
	tweet_id=str(tweet_id)

	for pair in loc_list:
		tid, loc = json.loads(pair) 
		if tid==tweet_id: return loc


if __name__ == '__main__':
	# combine_files("5.Get NER Entities", "2020-04-10")
	from sys import argv
	print(get_tweet_by_id(argv[1]))