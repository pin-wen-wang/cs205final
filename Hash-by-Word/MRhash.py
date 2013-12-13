"""
Created on Sun Nov  3 10:43:06 2013

** UPDATE 12/12/13: Obsolete code. Before I would add line numbers to texts like Pride and Prejudice, and this code takes such a text with line numbers as input. Now MRhash_word.py is used. The letsHash function defined here is the only reason I use this file. **

Purpose: Preprocessing ONE text: Hash each word
  --Input: Text with line numbers
    to add line numbers on command line:
      awk '{printf("%5d : %s\n", NR,$0)}' PrideAndPrejudice.txt > PPwithNum.txt
      *check the new txt for weird things

  --Output: (key,val) = (line number, ordered list of hashed words (numbers))


Run MR job on command line:
  python MRhash.py < PPwithNum.txt > letsdoPP.txt


Further consideration:
  --Empty lines (newline char?) get hash value of 0. Probs want to not hash these in first place

  --Is the hashing function okay? Using prime number q = 1009

  --I remove punc from text but leave numbers. should d = 26 + 10?

"""


import operator, string
from mrjob.job import MRJob



def letsHash(pat,q=1009,d=26):
    """Hash function"""

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
        """ Emit line number and hashVal of each word (with id=wordNum) """

        # "   12 : " to "12"
        lineNum = ''.join(c for c in line[:8] if c.isdigit())

        # where actual text starts based on awk
        text = line[8:]

        # to order hashed words in reducer, emit word position & line number
        for wordNum, word in enumerate(text.split(' ')):

            word = word.translate(string.maketrans("",""), string.punctuation).upper()

            yield lineNum, [wordNum, letsHash(word)]


    def sortHashed_reducer(self, lineNum, values):

        """ Sort the hashVals for words on the same line"""

        sortedVals = sorted(list(values), key=operator.itemgetter(0))
        hashVals = [val[1] for val in sortedVals]

        yield (int(lineNum), hashVals)
        # i think 0 is no text




if __name__ == '__main__':
    processText.run()
