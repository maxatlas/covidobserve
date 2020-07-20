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
from graph_building import get_knowledge_graph

def e2docs(entity_text, fulltext_folder, timeblock, word_index_dict):
	'''
		entitiy text: string.
		timeblock: string with delim=",", e.g. "2020-03-29,2020-03-30"

		output: [full_texts]

		TESTED.

	'''

	full_texts = []
	for date in timeblock.split(","): full_texts+=json.load(open(p.join(entity_folder, "%s.json"%date)))

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

	text+="\n"
	# text=text_newline(text) #not working ideally after testing.
	return text

def texts2docs(texts, timeblock, to_folder):
	'''
		texts: [text]. Contribute to a peaking entity of a certain time block.
		text: string refers to a single full_text attribute of tweet JSON.
		timeblock: string

		output:
		files under 7.N&NPs with each line being N&NPs
		Use ToPMine/TopicalPhrases/run.sh to change config for ToPMine algorithm

		TESTED

	'''
	def subrun(command): return subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE).communicate()[0]
	

	input_folder, file_name, curdir = "ToPMine/rawFiles", "cur.txt", getcwd()
	file = open(p.join(input_folder, file_name),"w+")
	file.writelines([alter_text(text) for text in texts])

	chdir(p.join(curdir, "ToPMine/TopicalPhrases")) #change working directory
	command = "./run.sh %s %s"%(to_folder, timeblock) #run run.sh
	subrun(command) #Use ToPMine/TopicalPhrases/run.sh to debug if any

	chdir(curdir) #change back to the correct directory

def nnps2docs(timeblock, nnps_folder):
	def alter_token(token):
		token = re.sub("\n", "", token)
		token = re.sub("   ", " ", token)
		token = re.sub("ERROR", "", token)
		return token

	def remove_ERROR(docs):
		doc = re.sub(",ERROR,", ",", "|".join(docs))
		return doc.split("|")

	nnps, docs = open(p.join(nnps_folder, "%s.txt"%timeblock)).readlines(), []
	nnps = remove_ERROR(nnps)
	for nnp in nnps: docs.append([alter_token(token) for token in nnp.split(",")])

	return docs

def get_key_graph(timeblock, nnps_folder, save_folder):
	kgraph=get_knowledge_graph(timeblock, docs=nnps2docs(timeblock, nnps_folder))
	if save_folder: json.dump(kgraph, open(p.join(save_folder, "%s.json"%timeblock)))
	return kgraph

if __name__ == '__main__':
	get_key_graph("2020-03-28")