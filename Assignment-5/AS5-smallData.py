# Task 1
import re
import numpy as np

# load up the small dataset
# https://s3.amazonaws.com/chrisjermainebucket/comp330_A5/TrainingDataOneLinePerDoc.txt
#corpus = sc.textFile("s3://chrisjermainebucket/comp330_A5/TestingDataOneLinePerDoc.txt")
corpus = sc.textFile("s3://chrisjermainebucket/comp330_A5/SmallTrainingDataOneLinePerDoc.txt")
#corpus = sc.textFile("s3://chrisjermainebucket/comp330_A5/TrainingDataOneLinePerDoc.txt")
# each entry in validLines will be a line from the text file
validLines = corpus.filter(lambda x: 'id' in x)
# now we transform it into a bunch of (docID, text) pairs
keyAndText = validLines.map(
    lambda x: (x[x.index('id="') + 4:x.index('" url=')], x[x.index('">') + 2:]))

# now we split the text in each (docID, text) pair into a list of words
# after this, we have a data set with (docID, ["word1", "word2", "word3", ...])
# we have a bit of fancy regular expression stuff here to make sure that we do not
# die on some of the documents
regex = re.compile('[^a-zA-Z]')
keyAndListOfWords = keyAndText.map(
    lambda x: (str(x[0]), regex.sub(' ', x[1]).lower().split()))

# now get the top 20,000 words... first change (docID, ["word1", "word2", "word3", ...])
# to ("word1", 1) ("word2", 1)...
allWords = keyAndListOfWords.flatMap(lambda x: ((j, 1) for j in x[1]))

# now, count all of the words, giving us ("word1", 1433), ("word2", 3423423), etc.
allCounts = allWords.reduceByKey(lambda a, b: a + b)

# and get the top 20,000 words in a local array
# each entry is a ("word1", count) pair
topWords = allCounts.top(20000, lambda x: x[1])
# and we'll create a RDD that has a bunch of (word, dictNum) pairs
# start by creating an RDD that has the number 0 thru 20000
# 20000 is the number of words that will be in our dictionary
twentyK = sc.parallelize(range(20000))

# now, we transform (0), (1), (2), ... to ("mostcommonword", 0) ("nextmostcommon", 1), ...
# the number will be the spot in the dictionary used to tell us where the word is located
# A bunch of (word, posInDictionary) pairs
dictionary = twentyK.map(lambda x: (topWords[x][0], x))

top_twentyK_words = dictionary.collectAsMap()

# find targeted word
for requestWord in ["applicant", "and", "attack", "protein", "car"]:
    rank_list = dictionary.lookup(requestWord)
    if rank_list:
        print(f'{requestWord}: {rank_list[0]}')
    else:
        print(f'{requestWord}: -1')


#TODO:: Task 2

# compute TF-IDF



#>>> WordsFlatInFile.top(2) => [('zzzzzzt', '20_newsgroups/rec.sport.baseball/104569'), ('zzzzzz', '20_newsgroups/rec.sport.hockey/53841')]
WordsFlatInFile = keyAndListOfWords.flatMap(lambda x: ((word, x[0]) for word in x[1]))

#>>> TopTwentyKWordsInFile.top(2) => [('zz', ('20_newsgroups/talk.politics.guns/54380', 1)), ('zz', ('20_newsgroups/talk.politics.guns/54380', 1))]
TopTwentyKWordsInFile = WordsFlatInFile.join(dictionary)
# topTwentyKWords.top(2) => [('20_newsgroups/talk.religion.misc/84570', 19589), ('20_newsgroups/talk.religion.misc/84570', 16880)]
topTwentyKWords = TopTwentyKWordsInFile.map(lambda x: (x[1][0], x[1][1]))
# wordsIdxsInFile.top(2) => [('20_newsgroups/talk.religion.misc/84570', <pyspark.resultiterable.ResultIterable object at 0x7f77bb9fd6d0>), ('20_newsgroups/talk.religion.misc/84569', <pyspark.resultiterable.ResultIterable object at 0x7f77bb9fd810>)]
wordsIdxsInFile = topTwentyKWords.groupByKey()
wordsIdxsInFileArray = wordsIdxsInFile.map(lambda x: (x[0], np.array(list(x[1]))))
# [('AU990', array([  10,   10,   10, ..., 6086, 6086, 1774])), ('AU945', array([ 2044,     1,     1, ...,  9979, 15446, 15446]))]
wordsIdxsInFileArray.top(2)

def countFreq(wordFreq):
    ans = np.zeros(20000)
    for freq in wordFreq: ans[int(freq)] += 1
    return ans

freqInFile = wordsIdxsInFileArray.map(lambda x: (x[0], countFreq(x[1])))
isAppearedInFile = freqInFile.map(lambda x: (x[0], np.array([1 if i else 0 for i in x[1]])))
# isAppearedInFile.top(2) => [('AU990', array([1, 1, 1, ..., 0, 0, 0])), ('AU945', array([1, 1, 1, ..., 0, 0, 0]))]
#isAppearedInFile.top(2)
unifiedAppr = isAppearedInFile.map(lambda x: ("all_file", x[1]))
# allAppearance => [('all_file', array([3441, 3441, 3442, ...,    4,    1,   10]))]
allAppearance = unifiedAppr.reduceByKey(lambda x, y: x+y)
#allAppearance.top(1)
fileCnt = freqInFile.count()
# IDF
# array([2.90570974e-04, 2.90570974e-04, 0.00000000e+00, ...,
#        6.75751362e+00, 8.14380798e+00, 5.84122288e+00])
IDF = np.log(fileCnt/allAppearance.top(1)[0][1])

# freqInFileDict  => {'AU990': array([551., 282.,  85., ...,   0.,   0.,   0.]), ...}
freqInFileDict = freqInFile.collectAsMap()
# TF_All = freqInFile / freqInFile.sum()

# calculate TF-IDF for "AU990"
file1FreqCnt = np.array(freqInFile.lookup("AU990"))
TF = file1FreqCnt / file1FreqCnt.sum()
ans = np.multiply(TF, IDF)

# TF-IDF
def TF_IDF(fileFreq):
    TF = fileFreq / fileFreq.sum()
    TFIDF = np.multiply(TF, IDF)
    return TFIDF

def calcTFIDF(s):
    regex = re.compile('[^a-zA-Z]')
    # Input string to a bunch of words
    words = regex.sub(' ', s).lower().split()
    fileFreq = np.zeros(20000)
    for word in words:
        if word in top_twentyK_words.keys():
            fileFreq[top_twentyK_words[word]] += 1
    result = TF_IDF(fileFreq)
    return result



TF_IDF(freqInFileDict['AU990'])

inputData_x = freqInFile.map(lambda x: (x[0], TF_IDF(x[1])))
inputData_x = inputData_x.cache()
#inputData_x.top(2)

# does the same ting
# inputData_x = keyAndText.map(lambda x: (x[0], calcTFIDF(x[1])))
# inputData_x.top(2)

inputData_y = keyAndText.map(lambda x: (x[0], 1 if x[0][0:2] == "AU" else 0))
#inputData_y.top(2)

def gradient(x, w, c):
    # taught by TA - Max
    y = 1 if "AU" in str(x[0]) else 0
    thetha = w.dot(x[1])
    g = np.vectorize(lambda x: -x*y + x*(np.exp(thetha)/(1 + np.exp(thetha))))
    grad = g(x[1]) + 2 * c * w
    print(f"Gradient: {grad}")
    return grad

def neg_llh(x, w):
    # taught by TA - Max
    y = 1 if "AU" in str(x[0]) else 0
    thetha = w.dot(x[1])
    return -(y * thetha - np.log(1 + np.exp(thetha)))

def gd_optimize(inputData_x, w, c=0.01):
    # taught by TA - Max
    total_docs = inputData_x.count()
    lr = 1
    delta = 1
    cur_loss = inputData_x.map(lambda x: neg_llh(x, w)).reduce(lambda x,y: x+y) + c * np.linalg.norm(w)**2
    cur_epoch = 0
    while delta > 10e-4:
        cur_epoch += 1
        grad = inputData_x.map(lambda x: gradient(x, w, c)).reduce(lambda x,y: x+y) / total_docs
        w -= lr * grad
        nxt_loss = inputData_x.map(lambda x: neg_llh(x, w)).reduce(lambda x,y: x+y) + c * np.linalg.norm(w)**2
        delta = abs(nxt_loss - cur_loss)
        if nxt_loss > cur_loss:
            lr /= 2
        else:
            lr *= 1.1
        cur_loss = nxt_loss
        print("Epoch: ", cur_epoch, ", Loss: ", cur_loss)
    return w

w_init = np.random.randn(20000)/10
pre_trained = gd_optimize(inputData_x, w_init)

## save IDF for future usage
with open('train_IDF.npy', 'wb') as f:
    np.save(f, IDF)

IDF1 = np.load('train_IDF.npy')


'''
Ans's implementation
'''

import math

# A bunch of (word, docID) pairs
wordDictPair = keyAndListOfWords.flatMap(lambda x: ((j, x[0]) for j in x[1]))
# Join the two RDDs, you'll have a bunch of (word, (docID, posInDictionary)) pairs
wordPair = wordDictPair.join(dictionary)

# Get a bunch of (docid, (listOfAllDictonaryPos)) pairs
docIDIdxPair = wordPair.map(lambda x: (x[1][0], x[1][1]))
docIDAllIdxPair = docIDIdxPair.groupByKey()
docIDAllIdxList = docIDAllIdxPair.map(lambda x: (x[0], list(x[1])))

# Then finally, you will write a map () that will take that RDD and convert into the listOfAllDictonary
def transformNP(idxList):
    array = np.zeros(20000)
    for i in idxList:
        array[i] += 1
    return array
result = docIDAllIdxList.map(lambda x: (x[0], transformNP(x[1])))


def findWord(wordList):
    arr = np.zeros(20000)
    for idx, i in enumerate(wordList):
        if i > 0:
            arr[idx] += 1
    return arr


# IDF
def IDF(result):
    cnt = 0
    val = result.map(lambda x: ("key", findWord(x[1])))
    finalList = val.reduceByKey(lambda a, b: a + b)
    return finalList


# Calculate IDF values
size = result.count()
IDFList = IDF(result)
IDFArr = np.array(IDFList.lookup("key"))
IDFArr = np.log(size / IDFArr)


# TF-IDF
def TFIDF(arrCnt):
    TFArr = arrCnt / arrCnt.sum()
    finalResult = np.multiply(TFArr, IDF)
    return finalResult


def predictLabel(inputStr):
    regex = re.compile('[^a-zA-Z]')
    # Input string to a bunch of words
    words = regex.sub(' ', inputStr).lower().split()
    wordArr = np.zeros(20000)
    for word in words:
        if word in localDict.keys():
            wordArr[localDict[word]] += 1
    inputTFIDF = TFIDF(wordArr)
    return inputTFIDF



# TFIDF for all dictionaries
trainingInfo = keyAndText.map(lambda x: (x[0], predictLabel(x[1])))
Data = keyAndText.map(lambda x: (1 if x[0][0:2] == "AU" else 0, predictLabel(x[1])))
trainingData = Data.map(lambda x: (x[0], x[1][0]))

meanVec = trainingData.map(lambda x: ("key", x[1]))
meanVec = meanVec.reduceByKey(lambda a, b: a+b)
meanVec = meanVec.map(lambda x: x[1])
mean = meanVec.collect()[0] / size

varVec = trainingData.map(lambda x: ("key", 10000 * np.square(x[1] - mean)))
varVec = varVec.reduceByKey(lambda a, b: a+b)
varVec = varVec.map(lambda x: x[1])
var = np.sqrt(varVec.collect()[0] / size)
var[var == 0] = 1
training = trainingData.map(lambda x: (x[0], (x[1] - mean) / var))
training = training.cache()

training.take(1)

# x is the data set
# y is the labels
# w is the current set of weights
# c is the weight of the slack variables

# Evaluates and returns the gradient
def gradient(train, w, c):
    l2 = np.linalg.norm(w, 2)
    grad_per_row = train.map(lambda x: -1 * x[0] * x[1] + x[1] * (np.exp(np.dot(x[1], w)) / (1 + np.exp(np.dot(x[1], w)))) + c * w * 2)
    acc = np.zeros(20000)
    grad = grad_per_row.fold(acc, lambda a, b: a + b)
    print(f"Gradient: {grad}")
    return grad

from operator import add

# x is the data set
# y is the labels
# w is the current set of weights
# c is the weight of the slack variables

# Evaluates the loss function and returns the loss
def f(train, w, c):
    l2 = np.linalg.norm(w, 2)
    loss_per_row = train.map(lambda x: ("key", (-1 * x[0] * np.dot(x[1], w)) + np.log(1 + np.exp(np.dot(x[1], w))) + c * l2 * l2))
    loss = loss_per_row.reduceByKey(add).collect()
    loss = loss[0][1]
    print(f"Loss: {loss}")
    return loss

# performs gradient descent optimization, returns the learned set of weights
# uses the bold driver to set the learning rate
#
# x is the data set
# y is the labels
# w is the current set of weights  to start with
# c is the weight of the slack variable
def gd_optimize(train, w, c):
    rate = 0.001
    w_last = w + np.full(20000, 1.0)
    while (abs(f(train, w, c) - f(train, w_last, c))) > 10e-4:
        w_last = w
        w = w - rate * gradient(train, w, c)
        if f(train, w, c) > f(train, w_last, c):
            rate = rate * 0.5
        else:
            rate = rate * 1.1
        print(f(train, w, c))
    return w

np.random.seed(42)
w = np.random.normal(0, 0.1, size=(20000))
c = 0.01
w = gd_optimize(training, w, 0.01)

idx = np.argpartition(w, -50)[-50:]
output = list()

for key, value in localDict.items():
    if value in idx:
        output.append(key)
print(output)