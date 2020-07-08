'''
	Pipeline Phase 1-1
	Input: CrisisNLP data format path. A list of dictionaries indicating location data.
'''
import json
import time
import stanza

from twarc import Twarc
from os import listdir, path as p
from os import environ as e
from preprocess_config import filter_by_loc, filter_en, filter_by_au

def main(file_folder, file_name, loc_folder="2.Filter by Location", text_folder="4.Get Tweets", ner_folder="5.Get NER Entities", save_per_tweets=500000, ner_gpu=True):
	
	#init objects
	twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])
	
	#Step 1. Filter by Location
	print("\n%s\nStart filtering by location."%file_name)

	tweet_ids = []
	for i, tweet_meta in enumerate(open(p.join(file_folder, file_name))):
		if i%100000==0: print(i) #count

		tweet_meta = json.loads(tweet_meta)
		if filter_by_loc(tweet_meta) or filter_by_au(tweet_meta, ["place", "coordinates"]): tweet_ids.append(tweet_meta["tweet_id"]) #if satisfy the filtering condition, append tweet_id

	file_name = file_name[4:]

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
	
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner', use_gpu=ner_gpu)
	#Step 4. NER tagging
	print("Start NER tagging")
	
	#init variables
	i, page_number = 0,1

	#Dice the full_texts list in chunks signified by save_per_tweets. Process by rounds
	while i<len(full_texts): 
		tweets_limited = "\n\n".join((full_texts[i:save_per_tweets*page_number]))

		start = time.time()
		entities = pipe(tweets_limited).entities
		entities = [entity.to_dict() for entity in entities if entity.type!="CARDINAL"] #turn object Span to dict, so that it's JSON Serializable.
		end = time.time()

		print("\noutput length for round %i: %i" %(page_number, len(entities)))
		print("Pipeline takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600)) #report process taken how long.

		if not i: json.dump(list(entities), open("%s/%s.json" %(ner_folder, file_name[:-5]), "w"))

		else: json.dump(list(entities), open("%s/%s_%i.json" %(ner_folder, file_name[:-5], page_number-1), "w"))

		del tweets_limited, entities

		print("save for %s_%i with %i tweets." %(file_name[:-5], page_number, (save_per_tweets*page_number-i)))

		i+=save_per_tweets
		page_number+=1

		print("Done for %s." %(file_name))

	print("\nPipeline compelete for %s" %file_name)

if __name__ == '__main__':
	from sys import argv
	if len(argv)<3: Print("Please input source folder and file name.")
	main(argv[1], argv[2])