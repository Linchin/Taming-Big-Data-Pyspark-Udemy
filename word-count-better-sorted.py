from pyspark import SparkConf, SparkContext
import re


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/Book.txt")
words = lines.flatMap(normalize_words)
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x,y: x+y)
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
results = word_counts_sorted.collect()

for result in results:
    clean_word = result[1].encode('ascii', 'ignore')
    if clean_word:
        print('{}:\t\t{}'.format(clean_word, result[0]))