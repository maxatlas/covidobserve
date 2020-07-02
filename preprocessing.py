'''

'''
import stanza
import json

from pprint import pprint

delim = "\n----*****----\n"

def get_NER_list_index(NER_end_index, text):
	return len(text[:NER_end_index].split(delim))-1

def texts2NER(texts, exclude_type=[]):
	'''
		texts: [text]
		text: tweet full_text.

		return [[NER]]
	'''
	NER_list = []

	

	text = delim.join(texts)
	NERs = get_NERs(text)

	for e in NERs:
		e_list_i = get_NER_list_index(e.end_char, text)
		if e.type not in exclude_type:
			while len(NER_list)<e_list_i+1: NER_list.append([])
			NER_list[e_list_i].append(e)
	
	return NER_list

def get_NERs(text):
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner', use_gpu=True)
	return pipe(text).entities

def NER2texts(e, texts):
	text = delim.join(texts)
	i = get_NER_list_index(e.end_char, text)
	return texts[i]

if __name__ == '__main__':
	sample_texts = json.load(open("4.Get Tweets/2020-03-28.json"))[:10]
	l = texts2NER(sample_texts)
	e = l[9][1]
	out = NER2texts(e, sample_texts)
	print(e)
	print(out)