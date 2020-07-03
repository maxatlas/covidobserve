'''

'''
import stanza
import json
import re

from pprint import pprint
from utils import get_name

delim = " \n----*****----\n "

replacement = {
	"AU" : "Australia",
	"NZ" : "New Zealand"
}

def get_NER_list_index(NER_end_index, text):
	return len(text[:NER_end_index].split(delim))-1

def replace_all(NERs, replacement_dict):
	def replace_name(token):
		'''
		example:
		
		realDonaldTrump -> Donald Trump
		realdonaldtrump -> realdonaldtrump
		TESTED
		'''
		return get_name(token)

	def replace_by_dict(token):
		if token in replacement: token = replacement_dict[token]
		return token

	for NER in NERs:
		NER_text = NER.text
		for token in replace_name(NER_text): yield {
			"text":replace_by_dict(token),
			"start_char": NER.start_char,
			"end_char": NER.end_char,
			"type":NER.type
			}

def texts2NER(texts, exclude_type=[]):
	'''
		texts: [text]
		text: tweet full_text.

		return [[NER]] index corresponds to [text], hence traceable
		TESTED
	'''
	NER_list = []

	text = delim.join(texts)
	NERs = get_NERs(text)
	NERs = list(replace_all(NERs, replacement_dict=replacement))

	for e in NERs:
		e_list_i = get_NER_list_index(e.get("end_char"), text)
		if e.get("type") not in exclude_type:
			while len(NER_list)<e_list_i+1: NER_list.append([])
			NER_list[e_list_i].append(e.get("text"))
	return NER_list

def get_NERs(text):
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner', use_gpu=True)
	return pipe(text).entities

def NER2texts(e, texts):
	'''
		TESTED
	'''
	text = delim.join(texts)
	i = get_NER_list_index(e.end_char, text)
	return texts[i]

if __name__ == '__main__':
	sample_texts = json.load(open("4.Get Tweets/2020-03-28.json"))[:10]
	l = texts2NER(sample_texts)
	e = l[9][1]
	out = NER2texts(e, sample_texts)