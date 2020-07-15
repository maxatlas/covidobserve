'''
	Pipeline phase 3.

	- tweets divided to time blocks.
	- remove first differences.
	- identify peaking entities.
	
	Since rolling mean and std are required, manipulation at block-level is needed.
'''
import json
import numpy as np
import pandas as pd

from pprint import pprint
from os import listdir, path as p
from preprocessing import texts2NER, NER2texts
from graph_building import get_knowledge_graph
from pipeline_config import get_folder_names
from collections import defaultdict

def get_e_sigs(folder):
	'''
		entity significance in pandas.Dataframe format which can be transformed to csv easily.
		TESTED.

	'''
	def get_e_sigs(e_sigs, indexes, minimum=0.001):
		'''
			e_sigs: {e_text:significance #(count/sum)}
			
			Space and time optimization needed?

		'''
		assert len(e_sigs)==len(indexes)

		out = pd.DataFrame(data=[]).rename_axis("NERs")
		for name, e_sig in zip(indexes, e_sigs): 
			e_sig = pd.Series(e_sig).to_frame().rename_axis("NERs")
			e_sig.columns=[name]
			out = out.merge(e_sig, left_on="NERs", right_on="NERs", how="outer")
		
		out = out.fillna(0)
		num = out._get_numeric_data()
		num[num<minimum]=0
		
		return out

	files = sorted(listdir(folder))
	e_sigs = [json.load(open(p.join(folder, file)))["e_sigs_mean"] for file in files]
	indexes = [index[:-5] for index in files]
	
	return get_e_sigs(e_sigs, indexes)

def get_peaking_entities(X=5, Y=1):
	'''
		TESTED.

	'''
	from_folder, to_folder = get_folder_names()[5], get_folder_names()[6]

	e_sigs = get_e_sigs(from_folder)
	if to_folder: e_sigs.to_csv(p.join(to_folder, "e_sigs.csv"))

	e_sigs_rolling_mean = e_sigs.rolling(X).mean()
	if to_folder: e_sigs_rolling_mean.to_csv(p.join(to_folder, "e_sigs_rolling_mean.csv"))

	e_sigs_rolling_std = e_sigs.rolling(X).std()
	if to_folder: e_sigs_rolling_std.to_csv(p.join(to_folder, "e_sigs_rolling_std.csv"))
	
	out = e_sigs - e_sigs_rolling_mean > Y*e_sigs_rolling_std
	out = out.replace(False, np.nan)
	out = out.replace(1.0, True)
	if to_folder: out.to_csv(p.join(to_folder, "peaking_entities.csv"))

	return out


def divide2blocks(graphs, dates, days_per_block=7):
	'''
		e.g.
		input: graphs (sorted by dates) length=35, days_per_block=7, dates=sorted dates of the graph
		output: graphs length=35/7=5
			e_sigs, edge_weights are merged,
			doc_indexes:{word: index}
	'''
	def init_block():
		return {"e_sigs_mean":pd.Series([], dtype="float64"),
				"edge_weights":pd.Series([], dtype="float64"),
				"word_index_dict":defaultdict(list),
				"docs_length":0,
				"date":""}

	out, block=[], init_block()
	for i, graph in enumerate(graphs):
		if (i+1)%days_per_block==1: block=init_block()
		block['e_sigs_mean']=block['e_sigs_mean'].add(pd.Series(graph['e_sigs_mean'],dtype="float64"), fill_value=0)
		block['edge_weights']=block['edge_weights'].add(pd.Series(graph['edge_weights'],dtype="float64"), fill_value=0)
		for word in graph["word_index_dict"]: block["word_index_dict"][word]+=(np.array(graph["word_index_dict"][word])+block["docs_length"]).tolist()
		block['docs_length']+=graph['docs_length']
		block["date"]+=graph["date"]+","

		if (i+1)%days_per_block==0:
			block["e_sigs_mean"] = (block["e_sigs_mean"]/days_per_block).to_dict()
			block["edge_weights"] = (block["edge_weights"]/days_per_block).to_dict()
			block["date"]=block["date"][:-1]
			out.append(block)

	return out


if __name__ == '__main__':
	graphs = [json.load(open(p.join(get_folder_names()[5], file))) for file in sorted(listdir(get_folder_names()[5]))]
	dates = [file[:-5] for file in sorted(listdir(get_folder_names()[5]))]
	blocks = divide2blocks(graphs, dates)
	json.dump(blocks, open("samples/blocks.json", "w"))