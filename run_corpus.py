import findspark
# findspark.init()
findspark.init("/home/nikita/bin/spark-2.1.0-bin-hadoop2.7")

import sys
from corpus_vectorizer import CorpusVectorizer
from pyspark import SparkContext
from numpy import array
from math import sqrt
from pyspark.mllib.clustering import KMeans, KMeansModel

input_file = sys.argv[1]

sc = SparkContext()
input_rdd = sc.textFile(input_file)
vectorized_docs = CorpusVectorizer(input_rdd).vectorize_corpus()
clusters = KMeans.train(vectorized_docs, 2, maxIterations=10, initializationMode="random")


def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sqrt(sum([x**2 for x in (point - center)]))


WSSSE = vectorized_docs.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE))

clusters.save(sc, "KMeansModel")
sameModel = KMeansModel.load(sc, "target/org/apache/spark/PythonKMeansExample/KMeansModel")
