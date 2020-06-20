import json
import time
import stanza

from twarc import Twarc
from os import listdir, path as p
from os import environ as e
from preprocess_config import filter_loc, filter_en

'''
	Input: CrisisNLP data format path. A list of dictionaries indicating location data.
	TODO: twarc command returns empty generator.
'''


def main(file_folder, file_name, loc_folder="2.Filter by Location", text_folder="4.Get Tweets", ner_folder="5.Get NER Entities", save_per_tweets=50000):
	
	#init objects
	twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner')
	
	#Step 1. Filter by Location
	print("\n%s\nStart filtering by location."%file_name)

	tweet_ids = []
	for i, tweet_meta in enumerate(open(p.join(file_folder, file_name))):
		if i%100000==0: print(i) #count

		tweet_meta = json.loads(tweet_meta)
		if filter_loc(tweet_meta): tweet_ids.append(tweet_meta["tweet_id"]) #if satisfy the filtering condition, append tweet_id

	loc_file = "%s/%s" %(loc_folder, file_name)
	json.dump(tweet_ids, open(loc_file, 'w')) #File save
	print("Location file generated at %s." %loc_folder)

	#Step 2. Twarc Hydration
	print("Start Twarc hydation.")

	tweets = list(twarc.hydrate(tweet_ids))
	del tweet_ids #delete no longer needed variables

	#Step 3. Filter by English
	print("Start filter by English and get full_text.")
	full_texts = []
	for tweet in tweets: full_texts+=filter_en(tweet)
	del tweets #delete no longer needed variables

	json.dump(full_texts, open("%s/%s" %(text_folder, file_name), "w"))
	print("English tweets saved at %s" %text_folder)

	#Step 4. NER tagging
	print("Start NER tagging")
	
	#init variables
	i, page_number = 0,1

	#Dice the full_texts list in chunks signified by save_per_tweets. Process by rounds
	while i<len(full_texts): 
		tweets_limited = "\n\n".join((full_texts[i:save_per_tweets*page_number]))

		start = time.time()
		entities = pipe(tweets_limited).entities
		entities = [entity.to_dict() for entity in entities] #turn object Span to dict, so that it's JSON Serializable.
		end = time.time()
		print("Pipeline takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600)) #report process taken how long.

		print("output length for round %i: %i" %(page_number, len(entities)))

		if not i: json.dump(list(entities), open("%s/%s.json" %(ner_folder, file_name[:-5]), "w"))

		else: json.dump(list(entities), open("%s/%s_%i.json" %(ner_folder, file_name[:-5], page_number-1), "w"))

		del tweets_limited, entities

		print("save for %s_%i of %i tweets." %(file_name[:-5], page_number, len(tweets_limited.split("\n\n"))))

		i+=save_per_tweets
		page_number+=1

		print("Done for %s." %(file_name))

	print("\nPipeline compelete for %s" %file_name)

if __name__ == '__main__':
	main("samples", "samples_geo.json")