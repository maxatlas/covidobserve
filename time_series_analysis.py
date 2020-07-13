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
		TESTED.

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
		
		out = out.fillna(0)
		num = out._get_numeric_data()
		num[num<0.001]=0
		
		return out

	files = sorted(listdir(folder))
	e_sigs = [json.load(open(p.join(folder, file)))["e_sigs_mean"] for file in files]
	indexes = [index[:-5] for index in files]
	
	return get_e_sigs(e_sigs, indexes)

def get_peaking_entities(from_folder, Y=1, to_folder="6.Peaking Entities"):
	'''
		TESTED.

	'''
	e_sigs = get_e_sigs(from_folder)
	if to_folder: e_sigs.to_csv(p.join(to_folder, "e_sigs.csv"))

	e_sigs_rolling_mean = e_sigs.rolling(3).mean()
	if to_folder: e_sigs_rolling_mean.to_csv(p.join(to_folder, "e_sigs_rolling_mean.csv"))

	e_sigs_rolling_std = e_sigs.rolling(3).std()
	if to_folder: e_sigs_rolling_std.to_csv(p.join(to_folder, "e_sigs_rolling_std.csv"))
	
	out = e_sigs - e_sigs_rolling_mean > Y*e_sigs_rolling_std
	out = out.replace(False, np.nan)
	out = out.replace(1.0, True)
	if to_folder: out.to_csv(p.join(to_folder, "peaking_entities.csv"))

	return out

if __name__ == '__main__':
	get_peaking_entities("5.Graphs")