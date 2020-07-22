from utils import ner_sent, get_loc, alter_text, alter_token
from pprint import pprint
from pipeline_config import filter_by_au, filter_by_city, filter_by_loc, filter_en
from graph_building import get_edge_weights_all_docs
from collections import defaultdict
import time, json

def test_alter_token():
	token="America🇺🇸"
	# token="H&amp;M"
	return alter_token(token)

def test_alter_text():
	text="It's #ourpleasure to have invited @realDonaldTrump and @NaciDanison!H&amp;M #this"
	return alter_text(text)

def test_get_edge_weights():
	docs=[["1", "2"], ["2", "3", "4"], ["5", "1"]]
	out = get_edge_weights_all_docs(docs)
	print(out)

def test_e2docs():
	from topic_summarization import e2docs
	
	date, e="2020-04-02", "Chinese Nationals"
	out=e2docs(e, date, json.load(open("5.Graphs/%s.json"%date))['word_index_dict'])
	pprint(out)
	print(len(out))

def test_divide2blocks():
	from time_series_analysis import divide2blocks
	
	graphs = [json.load(open(p.join(get_folder_names()[5], file))) for file in sorted(listdir(get_folder_names()[5]))]
	dates = [file[:-5] for file in sorted(listdir(get_folder_names()[5]))]
	blocks = divide2blocks(graphs, dates)
	json.dump(blocks, open("samples/blocks.json", "w"))

def get_tags(sentence):
	tags = pos_tag(TweetTokenizer().tokenize(sentence))
	tags = st.tag([tag[0] for tag in tags])

	tags = [tag[0] for tag in tags if tag[1]!="O"]
	out = tags
	return out


def compare_stanza_gpu_cpu(tweets):
	import stanza
	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner')

	start=time.time()
	entities = pipe("\n\n".join(tweets)).entities
	end=time.time()

	gpu_time = end-start
	print("Pipeline takes %i hours %f seconds" %((end-start)//3600, (end-start)%3600))

	pipe = stanza.Pipeline(lang='en', processors='tokenize,ner', use_gpu=False)

	start=time.time()
	entities = pipe("\n\n".join(tweets)).entities
	end=time.time()

	cpu_time = end-start

	print("GPU takes %f seconds.\nCPU takes %f seconds. \nDifference: %f." %(gpu_time, cpu_time, cpu_time-gpu_time))

def compare_filter_results(file_path, au_only=False):
	
	print("\n"+file_path)
	lau, lcity, lloc, l = [], [], [], []
	for tweet_meta in open(file_path):
		tweet_meta = json.loads(tweet_meta)
		
		if filter_by_au(tweet_meta, ["place", "geo"], au_only=au_only): 
			l.append(tweet_meta)
			lau.append(tweet_meta['tweet_id'])
		if filter_by_city(tweet_meta, ["place", "geo"]): lcity.append(tweet_meta)
		if filter_by_loc(tweet_meta, au_only=au_only): lloc.append(tweet_meta['tweet_locations'])

	print("\nfilter by au: %i" %len(lau))
	print("filter by city only: %i" %len(lcity))
	print("filter by tweet location: %i" %len(lloc))
	pprint(l[:50])

def get_text_loc_pairs(file_path, depths=[], data=None, to_folder="samples"):
	'''
		how accurate are the tweet locations?
		SHOW full_text and tweet_locations depth1/2/3/4 randomly select # of tweets per depth. Observe patterns.
		Try to avoid creating a big hash table. It's expensive to maintain.
	'''
	filename = file_path.split("/")[-1].split("_")[-1]
	
	#tweet_ids = {depth:[id]} 
	tweet_ids, out=defaultdict(set), defaultdict(dict)
	loc_list, loc_dict =set(), {} #{depth: [locations]}


	#Get list of tweet ids that satisfy the depth condition.
	for tweet_meta in open(file_path):
		tweet_meta = json.loads(tweet_meta)

		for place in tweet_meta['tweet_locations']:
			if place['country_code']=="au": 
				tweet_ids[len(place)].add(tweet_meta['tweet_id'])
				# pair = json.dumps((tweet_meta['tweet_id'], tweet_meta['tweet_locations']))
				# loc_list.add(pair)
				loc_dict[int(tweet_meta['tweet_id'])] = json.dumps(tweet_meta['tweet_locations'])

	if not data:
		from utils import get_twarc_instance
		twarc = get_twarc_instance()
		#print the list length of depths
		print("Filter results:")
		for depth in tweet_ids: print("\t%i tweets of depth=%s" %(len(list(set(tweet_ids[depth]))), str(depth)))

		#hydrate the list
		from numpy import random
		print("Start hydrating...")
		for depth in tweet_ids:
			for t in twarc.hydrate(list(tweet_ids[depth])):
				tweet_id, text = filter_en(t) #filter the English tweets
				if text and tweet_id: out[depth][tweet_id]=text
			print("Tweet location depth=%s: %i tweets." %(str(depth), len(out[depth])))
		
		del tweet_ids
		
		#save the data
		json.dump(out, open("%s/full_text_locdepths_%s" %(to_folder, filename), "w"))
		data = out
		out = defaultdict(dict)

	else: data = json.load(open(data))

	print("Data loaded.")

	for depth in data:
		for tweet_id in data[depth]: out[depth][tweet_id] = {"text":data[depth][tweet_id], "tweet_locations":loc_dict[int(tweet_id)]}
		
	json.dump(out, open("%s/loc-text_pair_locdepths_%s" %(to_folder, filename), "w"))
	print("File saved at samples.")

def show_text_loc_pairs(pair_file_path, size=150):
	data = json.load(open(pair_file_path))
	for depth in data:
		print("Depth=%i"%depth)
		key_indexes = len(data[depth])

if __name__ == '__main__':
	print(test_alter_text())