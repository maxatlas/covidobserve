'''
	Pipeline phase 2.
	- calculate entity significance per doc.
	- calculate mean entity sig over all docs of a time block.
	- calculate std dev for entities over all docs.
	- calculate all combos of edge weights per doc.
	- calculate sum of edge weights over all docs of a time block.

	main(docs)
	docs: [[NERs]], output of texts2NER func from preprocessing.py 
	output: 
		mean entity sig over all docs
		sum of edge weights over all docs
'''

import json
import pandas as pd
import numpy as np

from pprint import pprint
from os import listdir, path as p
from pprint import pprint
from collections import defaultdict
from preprocessing import texts2NER, NER2texts

def get_NER_text(docs):
	'''
		docs:[[e]]
		return [[e.text]]
		TESTED.

	'''
	def get_text(doc):
		for e in doc: yield e.get("text")
	return [list(get_text(doc)) for doc in docs]

def get_NER_indexes(docs):
	'''
		docs: [[NER]]
		
		output: {NER entity text: [index]}
		index - full_text index corresponds to the full_text
		TESTED.

	'''
	print("Start getting NER indexes...")
	out = defaultdict(list)
	for i, NERs in enumerate(docs):
		for NER in NERs: out[NER].append(i)
	print("Done.")
	return out

def get_e_sig(doc):
	'''
		doc: [NER entity].
		return pd.Series of {token: normalized}.
		TESTED.

	'''
	return pd.Series(doc, dtype="object").value_counts(normalize=True)

def get_e_sigs_mean(docs):
	'''
		TESTED.

	'''
	print("Start getting sigs mean...")
	n = len(docs)
	out = pd.Series([],dtype="float64")
	for doc in docs: out = out.add(get_e_sig(doc), fill_value=0)
	print("Done.")
	return out/n

def get_edge_weights_per_doc(tf):
	'''
		tf: output of get_e_sig
		key: string of 2 keys
		TESTED.

	'''
	out = pd.Series([], dtype="float64")
	for i, key0 in enumerate(tf.index):
		for key1 in tf.index[i+1:]:
			key, value = json.dumps(list(set((key0, key1)))), tf[key0]+tf[key1]
			out = out.add(pd.Series({key:value}), fill_value=0)
	return out

def get_edge_weights_all_docs(docs):
	'''
		docs: [doc]
		doc: [NER entities].
		TESTED.

	'''
	print("Start getting edge weights...")
	out = pd.Series([], dtype="float64")
	for i, doc in enumerate(docs):
		print(i)
		tf = get_e_sig(doc)
		out = out.add(get_edge_weights_per_doc(tf), fill_value=0)
		if i%1000==0: print(i)
	print("Done.")
	return out

def get_knowledge_graph(texts=None, NERs=None, e_only=False, edge_only=False, save_NER=None, tweets_per_round=500000):
	'''
		Get knowledge graph.
		
		texts: [full_text]
		docs: [[NER]]
		save_NER: file/filename

		mean(entity size) over all docs.
		edge weight over all docs.
	
		save_NER: file path+name
		TESTED.

	'''
	if texts and not NERs:
		NERs = texts2NER(texts, tweets_per_round=tweets_per_round, include=True)
		if save_NER: json.dump(NERs, open(save_NER, "w"))
		
	if NERs or texts: docs = get_NER_text(NERs)

	if e_only: return get_e_sig_mean(docs)
	elif edge_only: return get_edge_weights_all_docs(docs)

	else: return {
				"e_sigs_mean": get_e_sigs_mean(docs).to_dict(),
				"edge_weights": get_edge_weights_all_docs(docs).to_dict(),
				"word_index_dict": get_NER_indexes(docs)
			}

def main(from_directory, to_directory, NER_directory=None):
	'''
		get_knowledge_graph for all files under directory.
	
	'''
	bti = len(listdir(to_directory))
	for i, file in enumerate(listdir(from_directory)):
		print(i, file)
		if i> bti or i==bti:
			save_NER = p.join(NER_directory, file) if NER_directory else None

			texts = json.load(open(p.join(from_directory, file)))
			print("Texts loaded for %s" %file)

			graph = get_knowledge_graph(texts=texts, save_NER=save_NER)
			print("Graph painted.")
			
			json.dump(graph, open(p.join(to_directory, file), "w"))
			print("Done for %s." %file)

			del graph #space management


if __name__ == '__main__':
	# main("4.Get Tweets", "6.Graphs", NER_directory="5.Get NER Entities")

	NERs = json.load(open("5.Get NER Entities/2020-03-28.json"))

	out = get_knowledge_graph(NERs=NERs)
	json.dump(out, open("6.Graphs/2020-03-28.json", "w"))