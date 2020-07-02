import pandas as pd
import numpy as np

def get_tf_table(doc):
	'''
		doc: [NER entity].
		return pd.Series of {token: counts}.
	'''
	return pd.Series(doc).value_counts()

def get_entity_significance(tf, entity):
	'''
		tf: output of get_tf_table.
	'''
	tf_entity = tf[entity]
	tf_all = tf.sum()
	return tf_entity/tf_all

def get_edge_weight_per_doc(e1, e2, tf):
	e1_tf, e2_tf = tf.get(e1), tf.get(e2)
	if e1_tf and e2_tf: return e1_tf+e2_tf
	else: return 0

def get_edge_weight_all_docs(e1, e2, docs):
	'''
		docs: [doc]
		doc: [NER entities].
	'''
	out = 0
	for doc in docs: out+=get_edge_weight_per_doc(e1, e2, get_tf_table(doc))
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
		"entity size":,
		"edge weight":
	}


if __name__ == '__main__':
	main()