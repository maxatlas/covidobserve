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
import networkx as nx

from collections import defaultdict
from pprint import pprint
from os import listdir, chdir, getcwd, path as p
from graph_building import get_knowledge_graph
from utils import alter_token, alter_text, replace_by_dict, replace_name
from textblob import TextBlob

def e2docs(entity_text, fulltext_folder, timeblock, word_index_dict):
	'''
		entitiy text: string.
		timeblock: string with delim=",", e.g. "2020-03-29,2020-03-30"

		output: [full_texts]

		TESTED.

	'''

	full_texts = []
	for date in timeblock.split(","): full_texts+=json.load(open(p.join(fulltext_folder, "%s.json"%date)))

	return [full_texts[i] for i in word_index_dict[entity_text]]


def remove_freq_tokens(docs):
	'''
		remove tokens with high document frequency (DF)
	'''
	def get_freq_tokens(docs):
		token_freq = defaultdict(int)
		for doc in docs: 
			for token in set(doc): token_freq[token]+=1/len(docs)
		pprint(token_freq)
		return token_freq

	out, freq_dict = [], get_freq_tokens(docs)
	for doc in docs:
		inner = []
		for token in doc:
			if freq_dict[token]<0.3: inner.append(token)
		out.append(inner)

	return out

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
	def subrun(command): return subprocess.Popen(shlex.split(command), stdout=subprocess.PIPE).communicate()[0]
	def preprocess(texts): 
		texts = [re.sub("RT ", "", alter_text(text)) for text in texts]
		texts = [" ".join([replace_by_dict(alter_token(replace_name(token))) for token in text.split(" ")])+"\n" for text in texts]
		return texts


	docs = [TextBlob(text).noun_phrases for text in preprocess(texts)]

	json.dump(docs, open(p.join(to_folder, "%s.json"%timeblock), "w"))

	return docs

def get_key_graph(timeblock, nnps_folder, save_folder):
	kgraph=get_knowledge_graph(timeblock, docs=json.load(open(p.join(nnps_folder, "%s.json"%timeblock))))
	if save_folder: json.dump(kgraph, open(p.join(save_folder, "%s.json"%timeblock), "w"))
	return kgraph

def graph2nx(graph):
	'''
		Turn graph to a format that allows plotting. output can be fed to plot_graph directly.
		graph is the output of get_knowledge_gragh()

	'''
	G = nx.Graph()
	edges, nodes = [tuple(json.loads(edge))for edge in graph["edge_weights"]], set()
	for edge in edges: nodes = nodes.union(set(edge))

	nodes.discard("australia")
	nodes.discard("coronavirus")

	G.add_nodes_from(nodes)
	G.add_edges_from(edges)

	return G

def get_groups(partition):
	out = defaultdict(list)
	for node in partition: out[partition[node]].append(node)
	return out

def plot_graph(G):
	import community as community_louvain
	import matplotlib.cm as cm
	import matplotlib.pyplot as plt
	import networkx as nx

	#first compute the best partition
	partition = community_louvain.best_partition(G)
	groups = get_groups(partition)
	# draw the graph
	pos = nx.spring_layout(G)
	pprint(groups)

	# color the nodes according to their partition
	cmap = cm.get_cmap('viridis', max(partition.values()) + 1)
	nx.draw_networkx_nodes(G, pos, partition.keys(), node_size=40,
	                       cmap=cmap, node_color=list(partition.values()))
	nx.draw_networkx_edges(G, pos, alpha=0.5)
	plt.show()

if __name__ == '__main__':
	from pipeline_config import get_folder_names
	from sys import argv
	# kgraph = get_key_graph("2020-04-04,2020-04-05,2020-04-06,2020-04-07,2020-04-08,2020-04-09,2020-04-10", get_folder_names()[7], save_folder=None)
	kgraph = json.load(open(p.join(get_folder_names()[8], "%s.json"%argv[1])))
	G = graph2nx(kgraph)
	plot_graph(G)