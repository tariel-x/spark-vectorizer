#!/usr/bin/python -tt
# -*- coding: utf-8 -*-
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer
import string
import sys
import csv

stemmer = SnowballStemmer('english')
stop = stopwords.words('english')

corpus = []

with open('input/input.csv', 'rb') as csvfile:
    datareader = csv.reader(csvfile, delimiter=',', quotechar='"')
    i = 0
    for row in datareader:
        if len(row) <= 6:
            continue
        if len(row[6]) == 0:
            continue
        corpus.append(row)
        i += 1

data = open('data/output.csv', 'w')

# TODO: take first two sentences, remove place and date

for line in corpus:
    title = line[0]
    news_text = line[6]
    sys.stdout.write(str(i)+',')
    i += 1
    text = ''.join([c for c in news_text if c not in string.punctuation])
    text = [w.lower() for w in text.split()]
    text = [stemmer.stem(w) for w in text if w not in stop]
    if len(text) > 1:
        data.write(title + "," + ','.join(text).encode('utf-8') + "\n")
data.close()
