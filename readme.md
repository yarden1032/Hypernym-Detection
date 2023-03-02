# Hypernym Detection

## Created by:
### Yarden Kantor : 207684838
### Roni Ram : 318384484
## Table of contents
* [General info](#general-info)
* [References](#references)
* [Statistics](#Statistics)
* [Project workflow and summary](#project-workflow-and-summary)


## General info
In this project we re-designed a research paper algorithm to follow map-reduce pattern and experiment its quality on a large-scale input.
The goal of the project is to automatically learn hypernym (is-a) relations from a text using a knowledge-based classifier.  

## References
Learning syntactic pattern for automatic hypernym discovery - https://ai.stanford.edu/~rion/papers/hypernym_nips05.pdf

Google syntactic N-grams corpus -  http://storage.googleapis.com/books/syntactic-ngrams/index.html

## Statistics

#### Communication

<key,value> pairs between mapper and reducer in Parse Corpus - 54752836 in bytes - 2654515668

<key,value> pairs between mapper and reducer in Create Vectors - 53540383 in bytes - 2586284958


#### Results

after running a classifier based on Naive Bayes algorithm the measures received were:

Precision: 0.7046979865771812

Recall: 0.9459459459459459

F1 Score: 0.8076923076923076

#### Analysis

True Positives: 
- www,web
- world,men
- work,point
- work,creation
- wool,hair

True Negatives:
- phase,r
- metabolism,energy 
- world,character
- winter,right
- wildness,disorder

False Positives: 
- love, past - love and past are two words that might be connected literature context , because our corpus is based on books they might appear in many sentences together, therefore their feature vector can reflect on true value.
- area, lebanon	- even-though those two noun are not defined hypernym, in some cases we can claim that lebanon is an area, and therefore the feature vector between those two world maybe reflected as a feature vector of hypernym.     
- june, country & browning, wall & foam, protection - in those three noun pair, we don't see the connection between the two words in each pair. therefore they might not appear together in many sentences and so their feature vectors are not indicative enough.   

False Negatives:
- work,employment & wit,humour &  wife, woman - 
  the meaning of the two words in each pair here is very similar and as a result they might not appear together in one sentence,and so we don't have enough information to create accurate feature vector between them.
- volunteer,men & wind,direction - in our opinion, the definition of those pairs as hypernym is a bit ambiguous. therefor the classifier defines them as hypernym in contrast to the data set.  


## Project workflow and summary
the project consists of two map reduce jobs and local classifier build using the WEKA package. 

### Parse Corpus
this job receives as input the syntactic n-grams corpus and creates dependency paths between each noun pairs in every sentence in the corpus. 
each mapper receives a line and send to the reducer as key a dependency path and as value a noun pair. 
the reducer concatenates for each dependency path a list of all noun pair connected by this path and outputs is if there is the length of this list is bigger then a threshold 
named dpmin. this threshold is user defined and should be received as input to the whole program.


### Create Vectors
this job has two mappers and one reducer. 
- the first mapper receives as input the output of the previous job. for each noun pair in the list, the mapper sends the reducer as key the noun pair and as value the dependency path. 
- the second mapper receives as input lines containing noun pairs and a known classification is the noun pair represents a hypernym or not. the mapper send the reducer 
 this info as is.
- the reducer creates for each noun pair it receives as key a feature vector where each entry in this vector represents a possible dependency path.
for each dependency path per noun pair, the reducer increases the value by the num of occurrences of this dependency path in the corresponding entry.
- the reducer outputs for each noun pair its feature vector and a boolean representing of this noun pair is hypernym or not. 

### Build Classifier and test
after the map reduce jobs are done, the system downloads the result from S3 bucket and parses it to csv format.
with this data, we create the WEKA classifier and test it using the 10-fold cross validation. 
the output of the system is the precision, recall and f1 measures of the testing.  

