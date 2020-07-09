'''
	Pipeline Phase 1-1
	Input: CrisisNLP data format path. A list of dictionaries indicating location data.

	TODO: split NER list to be less time-consuming. 
'''
import json
import time
import stanza

from twarc import Twarc
from os import listdir, path as p
from os import environ as e
from preprocess_config import filter_by_loc, filter_en, filter_by_au
from graph_building import get_knowledge_graph

def main(file_folder, file_name, loc_folder="2.Filter by Location", text_folder="4.Get Tweets", ner_folder="5.Get NER Entities", graph_folder="6.Graphs", save_per_tweets=50000, ner_gpu=True):
	
	#init objects
	twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])
	
	#Step 1. Filter by Location
	print("\n%s\nStart filtering by location."%file_name)

	tweet_ids = []
	for i, tweet_meta in enumerate(open(p.join(file_folder, file_name))):
		if i%100000==0: print(i) #count
		tweet_meta = json.loads(tweet_meta)
		if filter_by_loc(tweet_meta) or filter_by_au(tweet_meta, ["place", "coordinates"]): tweet_ids.append(tweet_meta["tweet_id"]) #if satisfy the filtering condition, append tweet_id

	file_name = file_name.split("_")[-1]

	print(file_name)
	
	loc_file = "%s/%s" %(loc_folder, file_name)
	json.dump(tweet_ids, open(loc_file, 'w')) #File save
	print("Location file generated at %s, with %i tweets." %(loc_folder, len(tweet_ids)))

	#Step 2. Twarc Hydration
	print("Start Twarc hydation.")

	start = time.time()
	tweets = list(twarc.hydrate(tweet_ids))
	end = time.time()
	print("Hydration takes %f seconds." %(end-start))
	del tweet_ids #delete no longer needed variables

	#Step 3. Filter by English
	print("Start filter by English and get full_text.")
	full_texts = []
	for tweet in tweets: full_texts.append(filter_en(tweet))
	tweet_number = len(full_texts)
	del tweets #delete no longer needed variables

	json.dump(full_texts, open("%s/%s" %(text_folder, file_name), "w"))
	print("English tweets saved at %s with %i tweets." %(text_folder, tweet_number))
	full_texts = [text for _,text in full_texts] #remove tweet_id
	
	#Step 4. NER tagging and Get Graphs
	print("Start NER tagging")
	
	start = time.time()
	graph = get_knowledge_graph(texts=full_texts, save_NER=p.join(ner_folder, file_name), tweets_per_round=tweets_per_round)
	end = time.time()


	print("Pipeline takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600)) #report process taken how long.

	json.dump(graph, open(p.join(graph_folder, file_name), "w"))
	print("Graph saved at %s." %(graph_folder))
	del graph

	print("Done for %s." %(file_name))

	print("\nPipeline compelete for %s" %file_name)

if __name__ == '__main__':
	from sys import argv
	if len(argv)<3: Print("Please input source folder and file name.")
	main(argv[1], argv[2])