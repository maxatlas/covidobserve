import string
import re

from nltk.stem.snowball import SnowballStemmer
from sklearn.base import BaseEstimator, TransformerMixin
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords as sw
from nltk.corpus import wordnet as wn
from nltk.tag import StanfordNERTagger
from nltk import WordNetLemmatizer
from nltk import wordpunct_tokenize
from nltk import sent_tokenize
from nltk import pos_tag

from pprint import pprint
from os import environ as e

# export CLASSPATH=~/Desktop/tweetobserve/lib/python3.8/site-packages/stanford-ner-4.0.0
# export STANFORD_MODELS=~/Desktop/tweetobserve/lib/python3.8/site-packages/stanford-ner-4.0.0/classifiers

class Preprocessor():
	def __init__(self):
		self.stopwords = list(sw.words('english'))
		self.stemmer = SnowballStemmer('english')
		self.lemmatizer = WordNetLemmatizer()
		self.st = StanfordNERTagger('english.all.3class.distsim.crf.ser.gz') 
		self.tagdict = {
			'N': wn.NOUN,
			'V': wn.VERB,
			'R': wn.ADV,
			'J': wn.ADJ
			}

		self.punct = set(string.punctuation)
		self.punct.remove('#')


	def tokenize(self, tweet, valid_tags= ["N", "V", "R", "J"]):

		for sentence in sent_tokenize(tweet):
			sentence = re.sub('http[:/.\w]+', 'a_url', sentence)
			sentence = sentence.lower()

			for token, tag in pos_tag(TweetTokenizer().tokenize(sentence)):
				#get tags
				token = self.lemmatizer.lemmatize(token, self.tagdict.get(tag[0], wn.NOUN))
				exclude=['→', '’', '\u201c', '\u201d', '‘', '—', '…', "”", "rt"]+list(self.stopwords)
				is_unicode = True if re.findall('[\u00a0-\uffff]', token) else False
				
				if (len(token)>1) and not is_unicode and token not in exclude and tag[0] in valid_tags : #exclude: the exclude list, single character/unicode, valid_tag only, doesn't contain any punct: and not any(char in self.punct for char in token)
					# token = self.stemmer.stem(token)
					if token: yield token

				continue

	def fit(self, X, y=None):
		"""
		Fit simply returns self, no other information is needed.
		"""
		return self

	def inverse_transform(self, X):
		"""
		No inverse transformation
		"""
		return X

	def transform(self, X, valid_tags = ["N", "V", "R", "J"]):
		"""
		Actually runs the preprocessing on each document.
		"""

		# return [
			# list(self.tokenize(doc)) for doc in X
		# ]
		out = []
		for doc in X:
			new_doc = list(self.tokenize(doc, valid_tags=valid_tags))
			if new_doc: out.append(new_doc)

		return out


if __name__ == '__main__':
	p = Preprocessor()
	texts = []
	from sys import argv

	tweet = "RT @ausgov: All travellers arriving in Australia will be required to undertake  mandatory 14 day self-isolation at designated facilities (s\u2026! \u201d. \u201c"

	# tweet = [argv[1]] #reading error for argv inputs
	tweet = [tweet]
	print(p.transform(tweet, valid_tags=["N"]))