"""
Master Slave Implementation For Distributing Text From Multiple Files in Corpus To Processes

mpiexec -n 2 python MasterSlave_chunkMultCorpus.py hashedfilenames.txt multipattern50.txt

Serially handles multiple texts
"""

from mpi4py import MPI
from MRhash import letsHash
from mpiRK_chunkCorpus import processMatches
from itertools import izip

import sys
import string
import numpy as np



KILL_TAG = 1
WORK_TAG = 0

##########
def master(fileNames, comm, q=1009):

  # open file that lists texts to be searched through (corpus)
  with open(fileNames,'r') as files:
    names = files.readlines()
    numTexts = len(names)

  for name in names:
    numTexts -= 1
    singleText(numTexts, name.replace('\n',''), comm)

  return

########



########
def singleText(moreTexts, fileName, comm):
  """Distribute work to slaves from text in fileName"""

  hashedData = []

  try:
    with open(fileName,'r') as txtfile:
      for line in txtfile:
        # convert from string to list of tuples of form (lineNum, [hashed text])
        lineNum, sep, data = line.partition('[')
        hashedLine = [int(x) for x in data[:-2].split(", ")] # don't include ]
        hashedData.append((int(lineNum), hashedLine))

  # if file isn't in current directory - edit fullfilename as necessary
  except IOError:
    fullfilename = '../txt/'+fileName
    with open(fullfilename,"r") as txtfile:
      for line in txtfile:

        # convert from string to list of tuples of form (lineNum, [hashed text])
        lineNum, sep, data = line.partition('[')
        hashedLine = [int(x) for x in data[:-2].split(", ")] # don't include ]
        hashedData.append((int(lineNum), hashedLine))

  size = comm.Get_size()
  status = MPI.Status()
  cur_line = 0
  dummy = np.zeros(1, dtype=np.uint32)

  # number of lines of work to be done
  clen = len(hashedData)


  # Initialize by sending each slave one line of corpus text
  assert (size-1) < clen # num slaves < #lines in corpus
  for k in range(1,size):
    comm.send(hashedData[cur_line], dest=k, tag=WORK_TAG)
    cur_line += 1

  # While there is more work
  while cur_line < (clen-1):

    # Receive results from a slave
    comm.Recv(dummy, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

    # Send slave another unit of work
    comm.send(hashedData[cur_line], dest=status.source, tag=WORK_TAG)
    cur_line += 1


  # Get the outstanding results and kill slaves
  for k in range(1,size):
    comm.Recv(dummy, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)

    # only send kill command if this is the last text
    if not moreTexts:
      comm.send(hashedData[0], dest=status.source, tag=KILL_TAG)


  return

##########





##########
def slave(pHashed, pProcessed, m, comm):
  """ Match pattern to some chunk of corpus """

  status = MPI.Status()
  dummy = np.zeros(1, dtype=np.uint32)

  # Loop until kill command is received
  rank = comm.Get_rank()
  while True:
    # Recieve a message from the master
    hashedtxt = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)

    if status.tag == KILL_TAG:
      return

    # Do the work
    lineNum,txt = hashedtxt

    checkTxt(pHashed, pProcessed, txt, lineNum, m, rank)

    # Let master know you're done
    comm.Send(dummy, dest=0, tag=lineNum)

##########


##########
def checkTxt(pHashed, pProcessed, txt, lineNum, m, rank):
  '''Check for matches between pHashed (hashed pattern) and txt (one chunk of hashed corpus text)'''

  matches = []

  # for each m-tuple in corpus
  for k,txtMtuple in enumerate(izip(*[iter(txt[i:]) for i in xrange(m)])):

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

##########






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

  # is there at least one slave?
  assert comm.Get_size() > 1

  fileNames, pattxt = sys.argv[1:]

  m = 20 # num consecutive words that define plagiarism

  if comm.Get_rank() == 0:

    start_time = MPI.Wtime()
    master(fileNames, comm)
    end_time = MPI.Wtime()
    print "Time: %f secs" % (end_time - start_time)

  else:

    # this is the pattern in whih we're searching for plagiarism
    with open(pattxt,"r") as patfile:
      pat=patfile.read().replace('\n',' ')

    pHashed, pProcessed = hashPat(pat)

    slave(pHashed, pProcessed, m, comm)


