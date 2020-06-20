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

	print("Start filtering by location.")
	tweet_ids = []
	
	for i, tweet_meta in enumerate(open(p.join(file_folder, file_name))):

		if i%100000==0: print(i) #count

		tweet_meta = json.loads(tweet_meta)
		if filter_loc(tweet_meta): tweet_ids.append(tweet_meta["tweet_id"]) #if satisfy the filtering condition, append tweet_id

	loc_file = "%s/%s" %(loc_folder, file_name)
	json.dump(tweet_ids, open(loc_file, 'w'))
	print("Location file generated at %s." %loc_folder)

	print("Start Twarc hydation.")
	twarc = Twarc(e["TWITTER_API_KEY"], e["TWITTER_API_SECRET_KEY"], e["TWITTER_ACCESS_TOKEN"], e["TWITTER_ACCESS_TOKEN_SECRET"])

	tweets = list(twarc.hydrate(tweet_ids))

	print("Start filter by English and get full_text.")
	full_texts = []
	for tweet in tweets: full_texts+=filter_en(tweet)
	json.dump(full_texts, open("%s/%s" %(text_folder, file_name), "w"))
	print("English tweets saved at %s" %text_folder)

	print("Start NER tagging")
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner')
	
	i, page_number = 0,1

	while i<len(full_texts):
		tweets_limited = "\n\n".join((full_texts[i:save_per_tweets*page_number]))

		start = time.time()
		entities = pipe(tweets_limited).entities
		entities = [entity.to_dict() for entity in entities]

		end = time.time()
		print("Pipeline takes %f seconds." %(end-start))

		print("output length for round %i: %i" %(page_number, len(entities)))

		if not i: json.dump(list(entities), open("%s/%s.json" %(ner_folder, file_name[:-5]), "w"))

		else: json.dump(list(entities), open("%s/%s_%i.json" %(ner_folder, file_name[:-5], page_number-1), "w"))

		print("save for %s_%i of %i tweets." %(file_name[:-5], page_number, len(tweets_limited.split("\n\n"))))

		i+=save_per_tweets
		page_number+=1

		
		print("Done for %s." %(file_name))

	print("Pipeline compelete for %s" %file_name)

if __name__ == '__main__':
	main("samples", "samples_geo.json")