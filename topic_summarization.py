'''
	Pipeline phase 4.

	- Trace back from peaking entities to original documents.
	- Extract nouns and noun-phrases from original documents.
	- Create KeyGraph.
	- Create word clouds of top nouns and noun-phrases.

'''
import re
import json
import subprocess, shlex

from pprint import pprint
from os import listdir, chdir, getcwd, path as p
from pipeline_config import get_folder_names

def e2docs(entity_text, timeblock, word_index_dict):
	'''
		entitiy text: string.
		timeblock: string with delim=",", e.g. "2020-03-29,2020-03-30"

		output: [full_texts]

		TESTED.

	'''

	full_texts = []
	for date in timeblock.split(","): full_texts+=json.load(open(p.join(get_folder_names()[3], "%s.json"%date)))

	return [full_texts[i] for i in word_index_dict[entity_text]]

def alter_text(text):

	def text_newline(text, n=10):
		'''
			start new line for every n words.
		'''
		out, start_i = "", 0
		while start_i<len(text.split(" ")): 
			out+=" ".join(text.split(" ")[start_i:start_i+n])+"\n"
			start_i+=n
		return out

	text=re.sub("RT", "", text)
	text=re.sub("[\u00a0-\uffff]", "", text) #remove emojis
	text=re.sub("http[:/.\w]+", "", text) #remove web address
	text=re.sub("&amp", "&", text)

	# text+="\n"
	text=text_newline(text)
	return text

def texts2docs(texts, timeblock):
	'''
		texts: [text]. Contribute to a peaking entity of a certain time block.
		text: string refers to a single full_text attribute of tweet JSON.
		timeblock: string
	'''
	def subrun(command): return subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE).communicate()[0]
	

	input_folder, file_name, curdir = "ToPMine/rawFiles", "cur.txt", getcwd()
	file = open(p.join(input_folder, file_name),"w+")
	file.writelines([alter_text(text) for text in texts])

	chdir(p.join(curdir, "ToPMine/TopicalPhrases"))
	command = "./run.sh %s"%timeblock
	subrun(command) #phrases and corpus get

	chdir(curdir)

if __name__ == '__main__':
	texts = json.load(open("3.Tweet Texts/2020-03-28.json"))
	texts = [text[1] for text in texts]
	# texts_new = [alter_text(text) for text in texts]
	# for text in texts_new: print(text)
	texts2docs(texts, "test")