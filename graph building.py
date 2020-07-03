import json
import pandas as pd
import numpy as np

from pprint import pprint

def get_e_sig(doc):
	'''
		doc: [NER entity].
		return pd.Series of {token: normalized}.
		TESTED
	'''
	return pd.Series(doc).value_counts(normalize=True)

def get_e_sig_mean(docs):
	'''
		TESTED
	'''
	n = len(docs)
	out = pd.Series([],dtype="float64")
	for doc in docs: out = out.add(get_e_sig(doc), fill_value=0)

	return out/n

def get_edge_weights_per_doc(tf):
	'''
		tf: output of get_e_sig
		key: string of 2 keys
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

	'''
	out = pd.Series([], dtype="float64")
	for doc in docs: 
		tf = get_e_sig(doc)
		out = out.add(get_edge_weights_per_doc(tf), fill_value=0)
	return out

def get_knowledge_graph(docs):
	'''
		docs: [doc] of a time block.
		mean(entity size) over all docs.
		edge weight over all docs.
		每个doc的tf table求均值
		每个doc的所有edge weight求sum
		查pandas documentation，应该有简易办法 	
	'''


	return {
		"e_sig_mean": get_e_sig_mean(docs),
		"edge_weights": get_edge_weights_all_docs(docs)
	}


if __name__ == '__main__':
	from preprocessing import texts2NER
	sample_texts = json.load(open("4.Get Tweets/2020-03-28.json"))[:1000]
	l = texts2NER(sample_texts)
	out = get_edge_weights_all_docs(l)
	# out = get_e_sig_mean(l)
	pprint(out)
	# for NERs in l:
	# 	tf=get_e_sig(NERs)
	# 	ew = get_edge_weights_per_doc(tf)

	# 	pprint(ew)
	