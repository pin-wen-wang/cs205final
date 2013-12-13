# -*- coding: utf-8 -*-
"""
Created on Sun Nov 3 10:43:06 2013

Input is divided into K chunks. Hash them & return hashed text in order.

"""


import operator, string
from mrjob.job import MRJob



# Hash function
def letsHash(pat,q,d):

    patlen = len(pat)
    hashpat = 0

    for i in range(patlen):
        hashpat = (d*hashpat + ord(pat[i]))%q

    return hashpat



class processText(MRJob):

    def steps(self):
        "the steps in the map-reduce process"

        thesteps = [
           self.mr(mapper=self.word2hash_mapper,reducer=self.sortHashed_reducer)]

        return thesteps



    def word2hash_mapper(self,_,line):
        """ Emit word number and hashVal of word """

        process, _, text = line.partition(': ')

        # to order hashed words in reducer, emit word position & line number
        for wordNum, word in enumerate(text.split(' ')):
            word = word.translate(string.maketrans("",""), string.punctuation).upper()
            yield process, [wordNum, letsHash(word, q, d)]


    def sortHashed_reducer(self, process, values):

        """ Sort the hashVals for words belonging to the same process"""
        sortedVals = sorted(list(values), key=operator.itemgetter(0))
        hashVals = [val[1] for val in sortedVals]

        yield (int(process), hashVals)





if __name__ == '__main__':

    q = 1009 # large prime number
    d = 26 # number of letters in alphabet
    processText.run()