'''
	Pipeline 
	Stg1:
		Step 1. Filter by Location
		Step 2. Twarc Hydration
		Step 3. Filter by English
		Step 4. NER tagging
		Step 5. Get graphs
	Stg2:
		Step 6. Get peaking entities
		Step 7. Trace back to texts.
		Step 8. Get nouns and noun-phrases.
		Step 9. KeyGraph.
		Step 10. WordCloud.

'''
import json
import time
import stanza

from pipeline_config import get_folder_names
from os import listdir, path as p
from os import environ as e
from collections import defaultdict

file_folder, loc_folder, text_folder, ner_folder, graph_folder, pe_folder, nnps_folder, kgraph_folder, wc_folder = get_folder_names().values()

def stg1(file_name, data=None, start_from=1, end_at=10, tweets_per_round=20000):
	'''k
		Specify file_name if start_from=1
		Else input file_name and data.
		
		Stg1 - single file/date level run
		Step 1. Filter by Location
		Step 2. Twarc Hydration
		Step 3. Filter by English
		Step 4. NER tagging
		Step 5. Get graphs
	'''

	from twarc import Twarc
	from pipeline_config import filter_by_loc, filter_by_au, filter_en, get_full_text
	from graph_building import get_knowledge_graph
	from preprocessing import texts2NER
	
	assert end_at > start_from, "end_at (%i) needs to be bigger than start_from (%i)" %(end_at, start_from)
	
	
	def step1(file_folder, file_name, loc_folder):
		#Step 1. Filter by Location
		print("\n%s\nStart filtering by location."%file_name)

		tweet_ids = []
		for i, tweet_meta in enumerate(open(p.join(file_folder, file_name))):
			if i%100000==0: print("\tat %i"%i) #count
			tweet_meta = json.loads(tweet_meta)
			if filter_by_au(tweet_meta, filter_keys=["place", "geo", "user_location"]) or filter_by_loc(tweet_meta, depth=1, ratio=0.2): tweet_ids.append(tweet_meta["tweet_id"]) #if satisfy the filtering condition, append tweet_id
		
		file_name = file_name.split("_")[-1]
		loc_file = "%s/%s" %(loc_folder, file_name)
		json.dump(tweet_ids, open(loc_file, 'w')) #File save
		print("\tLocation file generated at %s, with %i tweets." %(loc_folder, len(tweet_ids)))

		return tweet_ids

	def step2(tweet_ids):
		#Step 2. Twarc Hydration
		print("\n\nStart Twarc hydation.")

		twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])

		start = time.time()
		tweets = list(twarc.hydrate(tweet_ids))
		end = time.time()
		print("\tHydration takes %f seconds." %(end-start))

		return tweets

	def step3(tweets, text_folder, file_name):
		#Step 3. Filter by English and get full text
		print("\n\nStart filter by English and get full_text.")
		full_texts = []
		for tweet in tweets:
			if filter_en(tweet): full_texts.append((tweet['id'], get_full_text(tweet))) #remove filter_en if not needed
		tweet_number = len(full_texts)

		json.dump(full_texts, open(p.join(text_folder, file_name), "w"))
		print("\tEnglish tweets saved at %s with %i tweets." %(text_folder, tweet_number))

		return full_texts

	def step4(full_texts, ner_folder, file_name, tweets_per_round):
		#Step 4. NER tagging
		print("\n\nStart NER tagging...")
		full_texts = [text for _,text in full_texts] #remove tweet_id

		start = time.time()
		NERs = texts2NER(texts=full_texts, tweets_per_round=tweets_per_round, include=True) #exclude "Australia"
		end = time.time()


		print("\tStep4 - NER tagging takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600)) #report process taken how long.

		json.dump(NERs, open(p.join(ner_folder, file_name), "w"))
		print("NERs saved at %s." %(ner_folder))

		return NERs

	def step5(NERs, graph_folder, file_name, date):
		#Step 5. Get graphs
		print("\n\nStart graph generation...")
		
		start = time.time()
		graph = get_knowledge_graph(date, NERs=NERs)
		end=time.time()

		print("\tStep5 - Graph generation takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600)) #report process taken how long.

		json.dump(graph, open(p.join(graph_folder, file_name), "w"))
		print("NERs saved at %s." %(graph_folder))

		return graph

	if start_from<2 and end_at>0: tweet_ids = step1(file_folder, file_name, loc_folder) #filter by location, get tweet_ids
	
	file_name = file_name.split("_")[-1]

	if start_from<3 and end_at>1:
		if start_from==2: tweet_ids=data #Hydrate tweet_ids, get tweets
		tweets = step2(tweet_ids)
		del tweet_ids #delete no longer needed variables

	if start_from<4 and end_at>2:
		if start_from==3: tweets = data
		full_texts = step3(tweets, text_folder, file_name) #filter by English, get tweets
		del tweets #delete no longer needed variables
	
	if start_from<5 and end_at>3:
		if start_from==4: full_texts = data
		NERs = step4(full_texts, ner_folder, file_name, tweets_per_round) #NER tagging, get NER entities
		del full_texts

	if start_from<6 and end_at>4:
		if start_from==5: NERs = data #graph building, get graphs
		graph = step5(NERs, graph_folder, file_name, file_name[:-5])
		del NERs

	print("\nPipeline compelete for %s" %file_name)



def stg2(X, Y, days_per_block, minimum):
	'''
		X: int, rolling window size
		Y: int, # of std away from mean to be identified as peaking entities
		days_per_block: int, days per time block
		minimum: float, minimum entity significance to be considered as peaking entities

		Stg2 - folder-level run
		Step 6. Get peaking entities
		Step 7. Trace back to texts.
		Step 8. Get nouns and noun-phrases.
		Step 9. KeyGraph.
		Step 10. WordCloud.

	'''
	from time_series_analysis import get_peaking_entities, divide2blocks
	from topic_summarization import e2docs, texts2docs, get_key_graph

	def get_NERi_dicts(graphs):
		out={}
		for graph in graphs: out[graph["timeblock"]]=graph["word_index_dict"]
		return out

	#step 6 - get peaking entities
	print("\n\nStart getting peaking entities...")
	graphs = [json.load(open(p.join(graph_folder, file))) for file in sorted(listdir(graph_folder))]
	if days_per_block>1: graphs = divide2blocks(graphs, days_per_block)
	PEs =  get_peaking_entities(graphs, X, Y, minimum, pe_folder, True)
	print("\tDone.")

	#step 7&8&9&10
	print("\nStart tracing back to texts, and getting nouns and noun-phrases...")
	NERi_dicts, out = get_NERi_dicts(graphs), defaultdict()
	
	for timeblock in PEs: #timeblock-level processing
		full_texts = []
		#Step 7 - Tracing back to texts
		for PE in PEs[timeblock]: full_texts += e2docs(PE, text_folder, timeblock, NERi_dicts[timeblock])

		#Step 8 - Getting N&NPs
		print("\nGetting Nouns and Noun-phrases for %s..."%timeblock)
		texts2docs([text for _,text in full_texts], timeblock, nnps_folder)
		print("\tDone.")
		#Step 9 - Creating KeyGraph
		print("\nGetting KeyGraphs for %s..."%timeblock)
		get_key_graph(timeblock, nnps_folder=nnps_folder, save_folder=kgraph_folder)
		print("\tDone.")
		#Step 10 - 


	print("\nDone.")




if __name__ == '__main__':
	stg2(5, 1, 1, minimum=0.01)
	# from sys import argv
	# if len(argv)<2: print("Please input file name. Change folder names from pipeline_config.py if need to.")
	# # main(argv[1])

	# data = json.load(open(p.join(argv[1], argv[2])))
	
	# main(argv[2], data, start_from=4)