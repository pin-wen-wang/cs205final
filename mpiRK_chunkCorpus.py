​# -*- coding: utf-8 -*-
"""
MPI where each process takes a portion of corpus (1 text) and searches for pattern throughout its chunk.

Using the Parallel RK paper to divide up text.

Input:
--preprocessed corpus (1 text) that's been divided into K (= #processes) parts using MRhash_word.py
--pattern - just normal text

Output: prints to screen the matches found

To do:
--check matches to ensure that the hash found a true match.
--fix the incorrect output when pattern has repeated chunks
--make processData faster so that it can append exisiting full chunks of texts rather than one word at a time

"""


import sys
import string
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from mpi4py import MPI
from regroupText import grouper
from MRhash import letsHash
from collections import Counter
from itertools import izip, groupby
from operator import itemgetter


########
def processData(hashedData, pat, m, rank, comm):

  """ Each process hashes the pattern and searches through part of corpus for matches of length m words"""


  # Hash words in pattern
  pHashed = []
  pProcessed = []
  matches = []

  for word in (pat.split()):
    new = word.translate(string.maketrans("",""), string.punctuation).upper()
    pProcessed.append(new)
    pHashed.append(letsHash(new, q=1009, d=26))


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

  processMatches(matches,m) # print out matches





def processMatches(matches,m):

  # data frame with tuple number and associated text
  df = pd.DataFrame({'tupleNum': [x[0] for x in matches],'txt': [x[1] for x in matches]})
  #print df.tupleNum.values

  # for text with consecutive tuples, I don't want to print out each of these tuples; I want to merge them so I'm not repeating the words in the middle. Try printing out in processData after a match has been found to see the difference.
  for key,group in groupby(enumerate(df.tupleNum), lambda (index, item): index-item):

    # turn group into list of consecutive tuples
    group = map(itemgetter(1), group)

    # can append whole m-sized quotes
    numFullQuotes = len(group)/m # check the math here

    matchedTxt = list(df[df.tupleNum==group[0]].txt.values)

    # append new words from consecutive tuples
    for val in group[1:]:

      # split the chunk of text into words
      words = df[df.tupleNum==val].txt.values[0].split()
      matchedTxt.append(words[-1])

    print 'Match found of length ', numFullQuotes, ' chunks (defined by m)'
    print ' '.join(matchedTxt)




########



if __name__ == '__main__':
  comm = MPI.COMM_WORLD
  size = comm.Get_size()
  rank = comm.Get_rank()

  m = 10 # pattern size!!

  hashedtxt, pattxt = sys.argv[1:]

  # this is the pattern in whih we're searching for plagiarism
  with open(pattxt,"r") as patfile:
    pat=patfile.read().replace('\n',' ') ### Clean up punc, etc?


  # this is the file with preprocessed hashed values of corpus text
  with open(hashedtxt, "r" ) as htxt:
    txt = htxt.readlines()
    mytxt = txt[rank]

    # convert from string to list of tuples of form (lineNum, [#...#])
    hashedData = []
    processNum, sep, data = mytxt.partition('[')
    assert int(processNum) == rank

    hashedLine = [int(x) for x in data[:-2].split(", ")] # don't include ] \n in data
    #hashedData.append((rank, hashedLine)) # create list of these tuples
    #print hashedData

    start_time = MPI.Wtime()
    processData(hashedLine, pat, m, rank, comm)
    end_time = MPI.Wtime()
    print "Time: %f secs (process %d)" % ((end_time - start_time), rank)


####
####
"""
  for txtChunk in izip(* [iter(hashedData[i:]) for i in xrange(m)] )):
      for i,patChunk in enumerate(izip(* [iter(pat[i:]) for i in xrange(m)] ))):

          breakpt = m # reset breakpt
          for j in xrange(m):
              if patChunk[j] != txtChunk[j]:
                breakpt = j
                break
        if breakpt == m: # was not redefined
          print 'Match found on line ',i
          print txtChunk

"""