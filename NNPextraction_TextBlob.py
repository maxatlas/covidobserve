import re
import json

from pprint import pprint
from os import listdir, chdir, getcwd, path as p
from utils import alter_token, alter_text, alter_topic_person, replace_by_dict, replace_name
from textblob import TextBlob

def texts2docs(texts, timeblock, to_folder):
	'''
		texts: [text]. Contribute to a peaking entity of a certain time block.
		text: string refers to a single full_text attribute of tweet JSON.
		timeblock: string
		
		remove_freq_tokens=True, so that high df terms are removed. tokens get effectively segregated into distinct topics.

		output:
		docs - [doc]
		doc - [token]

		TESTED

	'''
	def preprocess(texts): 
		texts = [alter_topic_person(alter_text(text)) for text in texts]
		texts = [" ".join([replace_by_dict(alter_token(replace_name(token))) for token in text.split(" ")])+"\n" for text in texts]
		return texts


	docs = [TextBlob(text).noun_phrases for text in preprocess(texts)]

	json.dump(docs, open(p.join(to_folder, "%s.json"%timeblock), "w"))

	return docs
