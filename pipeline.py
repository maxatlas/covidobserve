'''
	Pipeline steps:
		Step 1. Filter by Location
		Step 2. Twarc Hydration
		Step 3. Filter by English
		Step 4. NER tagging
		Step 5. Get graphs

'''
import json
import time
import stanza

from twarc import Twarc
from os import listdir, path as p
from os import environ as e
from pipeline_config import filter_by_loc, filter_en, filter_by_au, get_folder_names
from graph_building import get_knowledge_graph
from preprocessing import texts2NER
from time_series_analysis import get_peaking_entities

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
	#Step 3. Filter by English
	print("\n\nStart filter by English and get full_text.")
	full_texts = []
	for tweet in tweets: full_texts.append(filter_en(tweet))
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

def main(file_name, data=None, start_from=1, end_at=10, tweets_per_round=20000):
	'''
		Need to specify data if not start_from = 1
		
	'''
	assert end_at > start_from, "end_at (%i) needs to be bigger than start_from (%i)" %(end_at, start_from)
	
	#init objects
	file_folder, loc_folder, text_folder, ner_folder, graph_folder, _ = get_folder_names().values()

	if start_from<2 and end_at>0:
		tweet_ids = step1(file_folder, file_name, loc_folder)
	
	file_name = file_name.split("_")[-1]

	if start_from<3 and end_at>1:
		if start_from==2: tweet_ids=data
		tweets = step2(tweet_ids)
		del tweet_ids #delete no longer needed variables

	if start_from<4 and end_at>2:
		if start_from==3: tweets = data
		full_texts = step3(tweets, text_folder, file_name)
		del tweets #delete no longer needed variables
	
	if start_from<5 and end_at>3:
		if start_from==4: full_texts = data
		NERs = step4(full_texts, ner_folder, file_name, tweets_per_round)
		del full_texts

	if start_from<6 and end_at>4:
		if start_from==5: NERs = data
		graph = step5(NERs, graph_folder, file_name, file_name[:-5])
		del NERs

	print("\nPipeline compelete for %s" %file_name)

if __name__ == '__main__':
	from sys import argv
	if len(argv)<2: print("Please input file name. Change folder names from pipeline_config.py if need to.")
	main(argv[1])

	# data = json.load(open(p.join(argv[1], argv[2])))
	
	# main(argv[2], data, start_from=4)