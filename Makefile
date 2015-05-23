all: run

run:
	time spark-submit --driver-memory 6g --class com.ikaver.phrases.Phrases --master local[4] "target/spark-phrases-1.0-SNAPSHOT.jar" 1 100 50 4 4 1960 data/bigrams.txt data/unigrams.txt stopwords.txt 

