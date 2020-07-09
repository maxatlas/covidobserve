import json
import time

import stanza

from math import ceil
from os import listdir, path as pa
from sys import argv
from utils import *

def main(path="4.Get Tweets", ner_folder="5.Get NER Entities", from_i=0, to_i=-1, save_per_tweets=100000):
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner', use_gpu=True)

	to_path = "5.Get NER Entities"

	files = listdir(path)
	files.sort()

	for file_name in files[from_i:to_i]:
		print("\n"+file_name)

		tweets = json.load(open(pa.join(path, file_name)))
		
		i, page_number = 0,1
		
		print("Tweets loaded.")

		while i<len(tweets):
			tweets_limited = "\n\n".join((tweets[i:save_per_tweets*page_number]))

			start = time.time()
			entities = pipe(tweets_limited).entities 
			entities = [entity.to_dict() for entity in entities]

			end = time.time()
			print("\nPipeline takes %i hours %f seconds." %((end-start)//3600, (end-start)%3600))

			print("output length for round %i: %i" %(page_number, len(entities)))

			if not i: json.dump(list(entities), open("%s/%s.json" %(ner_folder, file_name[:-5]), "w"))

			else: json.dump(list(entities), open("%s/%s_%i.json" %(ner_folder, file_name[:-5], page_number-1), "w"))

			print("save for %s_%i of %i tweets." %(file_name[:-5], page_number, len(tweets_limited.split("\n\n"))))

			del entities, tweets_limited

			i+=save_per_tweets
			page_number+=1

		del tweets

			
		print("Done for %s." %(file_name))


if __name__ == '__main__':
	i1 = int(argv[1]) if len(argv)>1 else 0
	i2 = int(argv[2]) if len(argv)>2 else None
	save = int(argv[3]) if len(argv)>3 else 20000
	main(path="4.Get Tweets", from_i=i1, to_i=i2, save_per_tweets=save)
