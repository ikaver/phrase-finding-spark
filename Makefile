all: run

run:
	time spark-submit --driver-memory 4g --class com.ikaver.phrases.Phrases --master local "target/spark-phrases-1.0-SNAPSHOT.jar" 1 100 25 8 8 1960 data/bigrams.txt data/unigrams.txt stopwords.txt 

