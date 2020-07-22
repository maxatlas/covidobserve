import re
import time
import stanza
import json

from os import listdir, path as p
from pprint import pprint
from pipeline_config import filter_entity


def replace_name(e):
	'''
	e > e.text

	example: 
	
	realDonaldTrump > Donald Trump
	realdonaldtrump > realdonaldtrump
	TESTED
	'''
	def replace_name(text):
		out = re.findall("[A-Z][a-z]+", text)# *: Scott Morrison P M; +: Scott Morrison
		out = " ".join(out) if out else text
		return out

	qualified_types = ["PERSON", "FAC"] #for span type input

	if isinstance(e, str):
		if e.startswith("@"): return replace_name(e)
		return e
	else:
		if e.type in qualified_types: return replace_name(e.text)
		return e.text

def replace_by_dict(token):
	replacement_dict = filter_entity()['replacement']
	if token in replacement_dict: token = replacement_dict[token]
	return token


def alter_text(text):
	text = re.sub("&amp;", "&", text) #replace
	text = re.sub("&gt;", ">", text) #alter twitter format >
	text = re.sub("&lt;", "<", text) #alter twitter format <

	return text

def alter_topic_person(text):

	'''
	e.g.
	input: "It's #ourpleasure to have invited @person1 and @person2!"
	output: "It's #ourpleasure, to have invited @person1, and @person2!"

	Needed process for accurate Nouns & Noun Phrases extraction with TextBlob

	TESTED.
	'''

	hashtags = list(re.finditer("#([a-zA-Z0-9_])+", text))
	people = list(re.finditer("@([a-zA-Z0-9_])+", text))
	out = text
	for e in set(hashtags+people):
		if e.span()[1]<len(text):
			if text[e.span()[1]].isspace(): out = re.sub(e.group(), e.group()+",", out)
	return out

def alter_token(token):
	def remove_char_both_ends(token):
		if not token or ((token[-1].isalpha() or token[-1].isdigit()) and (token[0].isalpha() or token[0].isdigit())): return token
		elif token and not (token[-1].isalpha() or token[-1].isdigit()): return remove_char_both_ends(token[:-1])
		else: return remove_char_both_ends(token[1:])

	token = re.sub("[\u00a0-\uffff]", "", token) #remove_emojis
	token = re.sub("http[:/.\w]+", "", token) #remove web address
	token = re.sub("^RT", "", token)
	
	token = token[:-2] if token.endswith("'s") else token

	return remove_char_both_ends(token)


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