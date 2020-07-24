'''
	Pipeline phase 4.

	- Trace back from peaking entities to original documents.
	- Extract nouns and noun-phrases from original documents.
	- Create KeyGraph.
	- Create word clouds of top nouns and noun-phrases.

'''
import json
import networkx as nx

from collections import defaultdict
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
	for date in timeblock.split(","): full_texts+=json.load(open(p.join(fulltext_folder, "%s.json"%date)))

	return [full_texts[i] for i in word_index_dict[entity_text]]


def get_key_graph(timeblock, docs, save_folder=None):
	'''
		MAIN running file.
		
	'''
	kgraph=get_knowledge_graph(timeblock, docs=docs)
	if save_folder: json.dump(kgraph, open(p.join(save_folder, "%s.json"%timeblock), "w"))
	return kgraph

def graph2nx(graph):
	'''
		Turn graph to a format that allows plotting. output can be fed to plot_graph directly.
		graph is the output of get_knowledge_gragh()

	'''
	G = nx.Graph()
	nodes, edges = set(), []

	for edge in graph["edge_weights"]: edges.append(tuple(json.loads(edge)))
	for edge in edges: nodes = nodes.union(set(edge))
	
	G.add_nodes_from(nodes)
	G.add_edges_from(edges)

	return G

def get_groups(graph):
	import community as community_louvain

	'''
		MAIN running file.
		graph, output of get_key_graph()

	'''

	G = graph2nx(graph)
	partition = community_louvain.best_partition(G)
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
	
	pos = nx.spring_layout(G)

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
	groups = get_groups(kgraph)
	pprint(groups)
	G = graph2nx(kgraph)
	plot_graph(G)