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


def divide2blocks(e_sigs):
	'''
		e_sigs: {e_text:significance #(count/sum)}
		
		Space and time optimization needed?
	'''
	
	out = pd.DataFrame(data=[]).rename_axis("NERs")
	for e_sig in e_sigs: 
		e_sig = pd.Series(e_sig).to_frame().rename_axis("NERs")
		out = out.merge(e_sig, left_on="NERs", right_on="NERs", how="outer")
	
	return out

if __name__ == '__main__':
	folder = "5.Graphs"
	graphs = [json.load(open(p.join(folder, file)))["e_sigs_mean"] for file in listdir(folder)]
	graphs= graphs[:5]

	out = divide2blocks(graphs)
	pprint(out)