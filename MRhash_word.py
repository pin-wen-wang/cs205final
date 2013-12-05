# -*- coding: utf-8 -*-
"""
Created on Sun Nov 3 10:43:06 2013

Purpose: Preprocessing ONE text: Hash each word - keep track of order by each word.
  --Input: Text divided into K parts (K = num processes used in MPI)
    to add line numbers on command line:
      awk '{printf("%5d : %s\n", NR,$0)}' PrideAndPrejudice.txt > PPwithNum.txt
      *check the new txt for weird things

  --Output: (key,val) = (line number, ordered list of hashed words (numbers))


Run MR job on command line:
  python MRhash_word.py < PPwithNum.txt > letsdoPP.txt


Further consideration:
  --Empty lines (newline char?) get hash value of 0. Probs want to not hash these in first place

  --Improv hashing function? Using prime number q = 1009

  --I remove punc from text but leave numbers. should d = 26 + 10?

"""


import operator, string
from mrjob.job import MRJob



# Hash function
def letsHash(pat,q,d):
    #print pat
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