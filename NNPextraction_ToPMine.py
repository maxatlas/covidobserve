import re
import json
import subprocess, shlex
import networkx as nx

from pprint import pprint
from os import listdir, chdir, getcwd, path as p
from utils import alter_text, alter_topic_person, replace_by_dict, replace_name
from textblob import TextBlob


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
	def alter_token(token):
		token = re.sub("[\u00a0-\uffff]", "", token) #remove_emojis
		token = re.sub("http[:/.\w]+", "", token) #remove web address
		token = re.sub("^RT ", "", token)

		return token[1:] if token.startswith("@") or token.startswith("#") else token

	def preprocess(texts): 
		texts = [re.sub("\n", ". ", alter_topic_person(alter_text(text))) for text in texts]
		texts = [" ".join([replace_by_dict(alter_token(replace_name(token))) for token in text.split(" ")])+"\n" for text in texts]
		return texts

	def subrun(command): return subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE).communicate()[0]
	
	input_folder, file_name, curdir = "ToPMine/rawFiles", "cur.txt", getcwd()
	file = open(p.join(input_folder, file_name),"w+")
	file.writelines([text for text in preprocess(texts)])

	chdir(p.join(curdir, "ToPMine/TopicalPhrases")) #change working directory
	command = "./run.sh %s %s"%(to_folder, timeblock) #run run.sh
	subrun(command) #Use ToPMine/TopicalPhrases/run.sh to debug if any

	chdir(curdir) #change back to the correct directory

	return nnps2docs(timeblock, to_folder)

def nnps2docs(timeblock, nnps_folder):
	def alter_token(token):
		token = re.sub("\n", "", token)
		token = re.sub("   ", " ", token)
		token = re.sub("ERROR", "", token)
		return token.lower()

	def remove_ERROR(docs):
		doc = re.sub(",ERROR,", ",", "|".join(docs))
		return doc.split("|")

	nnps, docs = open(p.join(nnps_folder, "%s.txt"%timeblock)).readlines(), []
	nnps = remove_ERROR(nnps)
	for nnp in nnps: 
		nnp = re.sub("     ", ",", nnp)
		docs.append([alter_token(token) for token in nnp.split(",")])

	return docs

if __name__ == '__main__':
	from topic_summarization import plot_graph, graph2nx, get_key_graph, get_groups
	from sys import argv
	from os import path as p

	texts = json.load(open(p.join("3.Tweet Texts", "%s.json"%argv[1])))
	texts = [text for _, text in texts]
	docs = texts2docs(texts, argv[1], "7.N&NPs")
	kgraph = get_key_graph(argv[1], docs, None)
	groups = get_groups(kgraph)
	pprint(groups)
	G = graph2nx(kgraph)
	print("Start plotting graphs ...")
	plot_graph(G)