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

def get_e_sigs(folder):
	'''
		entity significance in pandas.Dataframe format which can be transformed to csv easily.

	'''
	def get_e_sigs(e_sigs, indexes):
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
		
		return out

	files = sorted(listdir(folder))
	e_sigs = [json.load(open(p.join(folder, file)))["e_sigs_mean"] for file in files]
	indexes = [index[:-5] for index in files]
	
	return get_e_sigs(e_sigs, indexes)

def get_peaking_entities(e_sigs):
	e_sigs_rolling_mean = e_sigs.rolling(3).mean()
	# out.to_csv("e_sigs_rolling_mean.csv")
	e_sigs_rolling_std = e_sigs.rolling(3).std()
	# e_sigs_rolling_std.to_csv("e_sigs_rolling_std.csv")
	
	out = e_sigs - e_sigs_rolling_mean > e_sigs_rolling_std
	# out.to_csv("peaking_entities.csv")

	return out

if __name__ == '__main__':
	pass