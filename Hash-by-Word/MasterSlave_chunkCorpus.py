"""
Master Slave

mpiexec -n 2 python MasterSlave_chunkCorpus.py FrankensteinHashed.txt ../corpus/Fpattern.txt


problem: if pattern is match between half of 1st line and next line.
  could split on paragraphs to make smaller chance of this happening (multipattern.txt). could have 1.5x as many lines.

"""

from mpi4py import MPI
#from regroupText import grouper
from MRhash import letsHash
from mpiRK_chunkCorpus import processMatches
#from collections import Counter
from itertools import izip, groupby
from operator import itemgetter

import sys
import string
import numpy as np
import pandas as pd


KILL_TAG = 1
WORK_TAG = 0

##########
def master(hashedData, comm, q=1009):

  # corpus info
  clen = len(hashedData)

  # pattern info
  #plen = len(pat)

  size = comm.Get_size()
  status = MPI.Status()
  cur_line = np.zeros(1, dtype=np.uint32)
  dummy = np.zeros(1, dtype=np.uint32)

  # Initialize by sending each slave one line of corpus text
  assert (size-1) < clen # num slaves < #lines in corpus
  for k in range(1,size):
    comm.send(hashedData[cur_line], dest=k, tag=WORK_TAG)
    cur_line += 1

  # While there is more work
  while cur_line < clen:

    # Receive results from a slave
    comm.Recv(dummy, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

    # Send slave another unit of work
    comm.send(hashedData[cur_line], dest=status.source, tag=WORK_TAG)
    cur_line += 1



  # Get the outstanding results and kill slaves
  for k in range(1,size):
    comm.Recv(dummy, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

    # send kill command
    comm.send(hashedData[0], dest=status.source, tag=KILL_TAG)


  return

##########



##########
def slave(pHashed, pProcessed, m, comm):
  """ Match pattern to some chunk of corpus """

  status = MPI.Status()
  dummy = np.zeros(1, dtype=np.uint32) # might not need to send round textLineNum

    # Loop until we are killed
  while True:
    # Recieve a message from the master
    hashedtxt = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
    lineNum,txt = hashedtxt

    if status.tag == KILL_TAG:
      return

    # Do the work
    checkTxt(pHashed, pProcessed, txt, lineNum, m)

    # Let master know you're done
    comm.Send(dummy, dest=0, tag=lineNum)

##########


##########
def checkTxt(pHashed, pProcessed, txt, lineNum, m):

  matches = []

  # for each m-tuple in corpus
  for k,txtMtuple in enumerate(izip(*[iter(txt[i:]) for i in xrange(m)])):
    #if k > 0: print k
    #ptiny
    # for m-tuples in pattern -- might just use izip here
    for i in range(len(pHashed)-m+1): # first word in seqs

      seq = pHashed[i:i+m]

      broken = m # not broken
      for j,hashedWord in enumerate(seq):

        if hashedWord != txtMtuple[j]:
          broken = j
          break


      if broken == m: # was not redefined
          #print (lineNum, k,' '.join(pProcessed[i:i+m]))
          matches.append((k,' '.join(pProcessed[i:i+m])))

  if len(matches) > 0:
    processMatches(matches,m) # print out matches

##########



########
def processMatches2(matches,m):
  print matches[0]
  # data frame with tuple number and associated text
  df = pd.DataFrame({'tupleNum': [x[0] for x in matches],'txt': [x[1] for x in matches]})
  #print df.tupleNum.values

  # for text with consecutive tuples, I don't want to print out each of these tuples; I want to merge them so I'm not repeating the words in the middle. Try printing out in processData after a match has been found to see the difference.
  for key,group in groupby(enumerate(df.tupleNum), lambda (index, item): index-item):

    # turn group into list of consecutive tuples
    group = map(itemgetter(1), group)
    #print 'group', (group)
    # can append whole m-sized quotes
    numFullQuotes = len(group)/m # check the math here

    matchedTxt = list(df[df.tupleNum==group[0]].txt.values)

    # append new words from consecutive tuples
    for val in group[1:]:

      # split the chunk of text into words
      words = df[df.tupleNum==val].txt.values[0].split()
      matchedTxt.append(words[-1])
      #print words[-1]

    print 'Match found of length!! ', numFullQuotes, ' chunks (defined by m)'
    print ' '.join(matchedTxt)
    print

########





##########
def hashPat(pat):
  """ Hash the pattern """

  # Hash words in pattern
  pHashed = []
  pProcessed = []

  for word in (pat.split()):
    new = word.translate(string.maketrans("",""), string.punctuation).upper()
    pProcessed.append(new)
    pHashed.append(letsHash(new, q=1009, d=26))

  return pHashed, pProcessed

##########





if __name__ == '__main__':
  comm = MPI.COMM_WORLD

  hashedtxt, pattxt = sys.argv[1:] # probs need to change

  m = 20 # num consecutive words that define plagiarism

  # this is the pattern in whih we're searching for plagiarism
  with open(pattxt,"r") as patfile:
    pat=patfile.read().replace('\n',' ')



  if comm.Get_rank() == 0:

    # this is the file with preprocessed hashed values of corpus text
    ins = open(hashedtxt, "r" )
    hashedData = []
    for line in ins:
      # convert from string to list of tuples of form (lineNum, [hashed text])
      lineNum, sep, data = line.partition('[')
      hashedLine = [int(x) for x in data[:-2].split(", ")] # don't include ] \n in data
      hashedData.append((int(lineNum), hashedLine)) # create list of these tuples
    ins.close()

    start_time = MPI.Wtime()
    master(hashedData, comm)
    end_time = MPI.Wtime()
    print "Time: %f secs" % (end_time - start_time)

  else:
    pHashed, pProcessed = hashPat(pat)

    #if comm.Get_rank() == 1: print "Patten length ", len(pHashed)
    slave(pHashed, pProcessed, m, comm)


