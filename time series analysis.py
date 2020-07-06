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

def divide2blocks(directory=None, NERs=None, NERs_stat=None):
	'''
		directory - folder where files of full_text by day reside.
	
	'''
	if directory:
		files = [p.join(file, directory) for file in listdir(directory)]
		blocks = [texts2NER(json.load(file)) for file in files]
		