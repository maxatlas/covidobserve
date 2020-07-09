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
	
	last = pd.DataFrame(data=[])
	rolling_means = None #How to do this in an efficient way?

	return df

if __name__ == '__main__':
	out = divide2blocks("4.Get Tweets", test=True)
	pprint(out)