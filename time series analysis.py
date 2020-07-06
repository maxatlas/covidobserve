'''
	Pipeline phase 3.

	- tweets divided to time blocks.
	- remove first differences.
	- identify peaking entities.
	
	Since rolling mean and std are required, manipulation at block-level is needed.
'''

from os import listdir, path as p
from preprocessing import texts2NER, NER2texts
from graph_building import get_knowledge_graph

def divide2blocks(directory):
	'''
		directory - folder where files of full_text by day reside.
		
		Space and time optimization needed?
	'''

	files = [p.join(file, directory) for file in listdir(directory)]
	texts = [json.load(file) for file in files]
	weighted_degrees_days = [get_knowledge_graph(text, e_only) for text in texts]
	rolling_means = None #How to do this in an efficient way?

	