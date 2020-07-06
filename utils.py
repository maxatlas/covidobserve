import re
import time
import stanza
import json

from os import listdir, path as p
from pprint import pprint

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
		print(e.text, out)

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
	combine_files("5.Get NER Entities", "2020-04-10")