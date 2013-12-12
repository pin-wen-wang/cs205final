# -*- coding: utf-8 -*-
"""
Serial Rabin Karp that uses MapReduce preprocessed hashed text for the corpus. The pattern is hashed in real time in this algorithm. Borrows functions defined in regroupText.py and MRhash.py

--Currently using a single hash instead of a double hash
--Counter isn't a reliable way to find matches since it doesn't care about order. Fix this.
"""


# inspiration from http://www.geeksforgeeks.org/searching-for-patterns-set-3-rabin-karp-algorithm#/

import sys, string, time
from mpiRK_chunkCorpus import processMatches
from MRhash import letsHash
from itertools import izip


d = 26 # number of characters in input alphabet?


def full_search(hashedData, pat, m=20):
  '''Take prehashed corpus text and hash the pattern text. Compare each line of 20 words in pattern with each line of 20 words in the corpus. Print line number and text for matches.'''

  # Hash words in pattern
  pHashed = []
  pProcessed = []
  matches = []

  for word in (pat.split()):
    new = word.translate(string.maketrans("",""), string.punctuation).upper()
    pProcessed.append(new)
    pHashed.append(letsHash(new))


  # for each m-tuple in corpus
  for k,txtMtuple in enumerate(izip(*[iter(hashedData[i:]) for i in xrange(m)])):

    # for m-tuples in pattern -- might just use izip here
    for i in range(len(pHashed)-m+1): # first word in seqs

      seq = pHashed[i:i+m]

      broken = m # not broken
      for j,hashedWord in enumerate(seq):

        if hashedWord != txtMtuple[j]:
          broken = j
          break


      if broken == m: # was not redefined
          matches.append((k,' '.join(pProcessed[i:i+m])))

  if len(matches) > 0:
    processMatches(matches,m) # print out matches


    ### preprocessing takes away magic of rolling hash


if __name__ == '__main__':

  hashedtxt, pattxt = sys.argv[1:]

  # this is the pattern in whih we're searching for plagiarism
  with open(pattxt,"r") as patfile:
    pat=patfile.read().replace('\n',' ') ### Clean up punc, etc?

  # this is the file with preprocessed hashed values of corpus text
  ins = open(hashedtxt, "r" )
  hashedData = []
  start = time.time()
  for line in ins:
    # convert from string to list of tuples of form (lineNum, [hashed text])
    lineNum, sep, data = line.partition('[')
    hashedLine = [int(x) for x in data[:-2].split(", ")] # don't include ] \n in data
    full_search(hashedLine, pat)
    #hashedData.append((int(lineNum), hashedLine)) # create list of these tuples
  ins.close()

  # hashedData is now a list. each element represents a line

  #q = 1079
  #q2 = 1011


  #full_search(hashedData, pat)
  print 'Time elapsed = ', time.time() - start
