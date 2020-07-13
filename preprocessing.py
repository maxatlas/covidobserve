'''
	Pipeline phase 1-2

'''
import stanza
import time
import json
import re

from pprint import pprint
from utils import get_name
from pipeline_config import filter_entity

filter_entity_dict=filter_entity()

delim, replacement, exclude_types, include_types = filter_entity_dict['delim'], filter_entity_dict['replacement'], filter_entity_dict['exclude_types'], filter_entity_dict["include_types"]

def get_NER_list_index(NER_end_index, text):
	return len(text[:NER_end_index].split(delim))-1

def replace_all(NERs, replacement_dict):
	def replace_name(e):
		'''
		e > e.text

		example: 
		
		realDonaldTrump > Donald Trump
		realdonaldtrump > realdonaldtrump
		TESTED
		'''
		return get_name(e)

	def replace_by_dict(token):
		if token in replacement: token = replacement_dict[token]
		return token
	
	def remove_RT(token):
		out = token[3:] if token.startswith("RT ") else token
		return out

	for NER in NERs:
		yield {
			"text":replace_by_dict(remove_RT(replace_name(NER))),
			"start_char": NER.start_char,
			"end_char": NER.end_char,
			"type":NER.type
			}

def texts2NER(texts, report=False, exclude=False, include=False, tweets_per_round=500000):
	'''
		texts: [text]
		text: tweet full_text.

		return [[NER]] index corresponds to [text], hence traceable
		TESTED
	'''

	i, page_number, NER_list, i2add = 0,1,[], 0 #i2add: alter start_char, end_char by this much
	texts_str = delim.join(texts)

	while i<len(texts): 
		print("\tRound %i"%page_number)
		text = delim.join(texts[i:tweets_per_round*page_number])

		start=time.time()
		NERs = get_NERs(text)
		end=time.time()
		print("\nOutput length for round %i: %i" %(page_number, len(NERs)))
		print("\t...takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600)) #report process taken how long.

		print("Start post processing...incl type filtering, start_char & end_char altering, to_dict...")
		NERs = list(replace_all(NERs, replacement_dict=replacement))

		exclude_types=filter_entity_dict["exclude_types"] if exclude else []
		include_types=filter_entity_dict["include_types"] if include else []

		count=0

		for e in NERs:
			e['end_char'], e['start_char'] = e['end_char']+i2add, e['start_char']+i2add
			e_list_i = get_NER_list_index(e.get("end_char"), texts_str)
			
			if (not include_types and e.get("type") not in exclude_types) or (not exclude_types and e.get("type") in include_types):
				while len(NER_list)<e_list_i+1: NER_list.append([])
				NER_list[e_list_i].append(e)
				count+=1
				if report: print(e['text'], e['type'])

		i2add+=len(text)
		i+=tweets_per_round
		page_number+=1

		del NERs
		
	print("\tDone with %i NER entities."%count)

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
	sample_texts = json.load(open("4.Get Tweets/2020-03-30.json"))
	sample_texts = [item[1] for item in sample_texts]
	# text = delim.join(sample_texts)
	# text = "@GladysB Stable door closed by @ScottMorrisonMP. \nHorse has bolted. \nAustralia has to contend with community transmission and recession due to fatal dithering and delay. \nhttps://t.co/maHJlRSTgy \n#covid19australia #coronavirus"
	# out = get_NERs(text)
	l = texts2NER(sample_texts, include=True, tweets_per_round=50000)
	pprint(l)
	# out = NER2texts(e, sample_texts)