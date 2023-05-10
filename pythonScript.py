from nltk.stem import *
import sys
stemmer = PorterStemmer()
plural = sys.argv[1]
single = stemmer.stem(plural)
print(single)