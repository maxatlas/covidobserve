'''
	Pipeline phase 4.

	- Trace back from peaking entities to original documents.
	- Extract nouns and noun-phrases from original documents.
	- Create KeyGraph.
	- Create word clouds of top nouns and noun-phrases.

'''

import json

from pprint import pprint
from os import listdir, path as p
from pipeline_config import get_folder_names

def e2docs(entity_text, timeblock, word_index_dict):
	'''
		entitiy text: string. 
		timeblocks: [timeblock]
		timeblock: string with delim=",", e.g. "2020-03-29,2020-03-30"

		output: [full_texts]

		TESTED.

	'''

	full_texts = []
	for date in timeblock.split(","): full_texts+=json.load(open(p.join(get_folder_names()[3], "%s.json"%date)))

	return [full_texts[i] for i in word_index_dict[entity_text]]

if __name__ == '__main__':
	date, e="2020-04-02", "Chinese Nationals"
	out=e2docs(e, date, json.load(open("5.Graphs/%s.json"%date))['word_index_dict'])
	pprint(out)
	print(len(out))