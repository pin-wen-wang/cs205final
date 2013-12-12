# -*- coding: utf-8 -*-
"""
Serial Rabin Karp that uses MapReduce preprocessed hashed text for the corpus. The pattern is hashed in real time in this algorithm. Borrows functions defined in regroupText.py and MRhash.py

--Currently using a single hash instead of a double hash
--Counter isn't a reliable way to find matches since it doesn't care about order. Fix this.
"""

#########################################
# http://www.geeksforgeeks.org/searching-for-patterns-set-3-rabin-karp-algorithm#/
# serial Rabin Karp algorithm
#########################################
import sys, string
from collections import Counter
from regroupText import grouper
from MRhash import letsHash


d = 26 # number of characters in input alphabet?




def full_search(hashedData,fulltxt, pat,q):
  '''Take prehashed corpus text and hash the pattern text. Compare each line of 20 words in pattern with each line of 20 words in the corpus. Print line number and text for matches.'''

  exclude = set(string.punctuation) # to massage text below

  # hash pattern in twenty-word lines
  for line_pat in pat:

    hashpat = []
    for word_pat in line_pat.split():

      # strip word of puncuation & captialize-- do more efficiently!
      word_pat = ''.join(ch.upper() for ch in word_pat if ch not in exclude)
      hashpat.append(letsHash(word_pat, q=1009, d=26))

    # compare hashed line to
    for line_corpus in hashedData:
      lineNum = line_corpus[0]
      hashcorpus = line_corpus[1]

      # if match
      if Counter(hashcorpus) == Counter(hashpat): # not sufficient: order should matter!!
        print "Match on line ", lineNum
        print txtList[lineNum-1] # line nums start at 1, python indexes at 0

    ### rolling hash is missing. i need to shift by one word. and thehash is supposed to

    ### preprocessing takes away magic of rolling hash


if __name__ == '__main__':

  hashedtxt, fulltxt, pattxt = sys.argv[1:]

  # this is the actual corpus text
  with open(fulltxt, "r") as txtfile:
    txt=txtfile.read().replace('\n', ' ') # replace newline with space

    # reshaping: 20 words per chunk
    grouped = grouper(20, txt.split(), "")

    txtList = []
    for chunk in list(grouped):
        txtList.append(' '.join(chunk))

  # this is the pattern in whih we're searching for plagiarism
  # need to reshape pattern too or do it on the fly
  with open(pattxt,"r") as patfile:
    pat=patfile.read().replace('\n',' ')
    #pat = pat.upper()

    # reshaping: 20 words per chunk
    grouped = grouper(20, pat.split(), "")

    patList = []
    for chunk in list(grouped):
        patList.append(' '.join(chunk))
    # "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"

    # patList is a list of strings of 20 words each



  # this is the file with preprocessed hashed values of corpus text
  ins = open(hashedtxt, "r" )
  hashedData = []
  for line in ins:
    # convert from string to list of tuples of form (lineNum, [hashed text])
    lineNum, sep, data = line.partition('[')
    hashedLine = [int(x) for x in data[:-2].split(", ")] # don't include ] \n in data
    hashedData.append((int(lineNum), hashedLine)) # create list of these tuples
  ins.close()

  # hashedData is now a list. each element represents a line


  #q = 1079
  #q2 = 1011


  full_search(hashedData,txtList, patList,q=1009)
