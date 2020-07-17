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
from preprocessing import texts2NER
from graph_building import get_knowledge_graph
from pipeline_config import get_folder_names
from collections import defaultdict

def divide2blocks(graphs, days_per_block):
	'''
		e.g.
		input: list, [graphs] (sorted by timeblocks) length=35, days_per_block=7
		output: list, [block] length=35/7=5
			e_sigs, edge_weights are merged,
			doc_indexes:{word: index}

		TESTED.

	'''
	def init_block():
		return {"e_sigs_mean":pd.Series([], dtype="float64"),
				"edge_weights":pd.Series([], dtype="float64"),
				"word_index_dict":defaultdict(list),
				"docs_length":0,
				"timeblock":""}

	out, block=[], init_block()
	for i, graph in enumerate(graphs):
		if (i+1)%days_per_block==1 or days_per_block==1: block=init_block()
		print(i, type(block["e_sigs_mean"]))
		block['e_sigs_mean']=block['e_sigs_mean'].add(pd.Series(graph['e_sigs_mean'],dtype="float64"), fill_value=0)
		block['edge_weights']=block['edge_weights'].add(pd.Series(graph['edge_weights'],dtype="float64"), fill_value=0)
		for word in graph["word_index_dict"]: block["word_index_dict"][word]+=(np.array(graph["word_index_dict"][word])+block["docs_length"]).tolist()
		block['docs_length']+=graph['docs_length']
		block["timeblock"]+=graph["timeblock"]+","

		if (i+1)%days_per_block==0:
			block["e_sigs_mean"] = (block["e_sigs_mean"]/days_per_block).to_dict() #normalize
			block["edge_weights"] = (block["edge_weights"]/days_per_block).to_dict() #normalize
			block["timeblock"]=block["timeblock"][:-1]
			out.append(block)

	return out

def get_peaking_entities(graphs, X, Y, minimum, to_folder, save):
	'''
		TESTED.
		X: int, sliding window size
		Y: int, # of std away from mean to be considered as peaking entities
		minimum: float, threshold significance to be PE
		to_folder: string, empty if won't save.
		save: bool.
		
		output: 
			{timeblock: [entity]}

	'''

	def remove_trend(df):
		'''
			df: output of get_e_sigs()
			Needs testing.
		'''
		for col in df.columns:
			to_remove = np.array([df[col].mean()]+df[col].to_list()[:-1])
			first_diff = abs(df[col].to_numpy()-to_remove)

			df[col] = df[col] * (first_diff>1*df[col].std())

		return df


	def get_e_sigs(graphs):
		'''
			graphs: [graph]
			graph: output of graph_building > get_knowledge_graph
			
			output: DataFrame with timeblocks as column and NER as row
			TESTED
		'''
		e_sigs, indexes=[graph["e_sigs_mean"] for graph in graphs], [graph["timeblock"] for graph in graphs]

		out = pd.DataFrame(data=[], dtype="float64").rename_axis("NERs")
		for name, e_sig in zip(indexes, e_sigs): 
			e_sig = pd.Series(e_sig).to_frame().rename_axis("NERs")
			e_sig.columns=[name]
			out = out.merge(e_sig, left_on="NERs", right_on="NERs", how="outer")
		
		out = out.fillna(0)
		return out

	def save_CSV(df, save_folder):
		df = df.replace(False, np.nan)
		df = df.replace(1.0, True)

		df.transpose().to_csv(p.join(save_folder, "peaking_entities.csv"))

	def get_pe(df):
		out = {}
		for timeblock in df.columns:
			entities = df[timeblock].loc[df[timeblock].values].index.to_list()
			if entities: out[timeblock] = entities
		return out

	e_sigs = get_e_sigs(graphs).transpose()
	if save: e_sigs.transpose().to_csv(p.join(to_folder, "e_sigs.csv"))
	e_sigs = remove_trend(e_sigs)
	if save: e_sigs.transpose().to_csv(p.join(to_folder, "e_sigs_remove_trends.csv"))

	e_sigs_rolling = e_sigs.rolling(X, min_periods=X//2, center=True)
	e_sigs_rolling_mean, e_sigs_rolling_std = e_sigs_rolling.mean(), e_sigs_rolling.std()
	if save: e_sigs_rolling_mean.transpose().to_csv(p.join(to_folder, "e_sigs_rolling_mean.csv"))
	if save: e_sigs_rolling_std.transpose().to_csv(p.join(to_folder, "e_sigs_rolling_std.csv"))
	
	out = (e_sigs * (e_sigs-e_sigs_rolling_mean>Y*e_sigs_rolling_std)) > minimum
	if save: save_CSV(out, to_folder)

	return get_pe(out.transpose())


def main(X=5, Y=1, days_per_block=1, minimum=0.001, save=True):
	'''
		days_per_block: if =7, 7 days to make up a time block.

	'''
	from_folder, to_folder = get_folder_names()[5], get_folder_names()[6]
	files = sorted(listdir(from_folder))
	graphs = [json.load(open(p.join(from_folder, file))) for file in sorted(listdir(from_folder))]
	if days_per_block>1: graphs = divide2blocks(graphs, days_per_block)

	return get_peaking_entities(graphs, X, Y, minimum, to_folder, save)

if __name__ == '__main__':
	out = main(save=False)
	pprint(out)