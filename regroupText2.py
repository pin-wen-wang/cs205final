# -*- coding: utf-8 -*-
"""
Purpose: Take a text (basename) and divide into K (#processes) pieces. or K >> #processes for master/slave. Then hash using map reduce

usage: python regroupText2.py Frankenstein K m

Input
--basename: name of text we're working with
--K: number of processes/chunks desired.
--m: number of matched, consecutive words to constitute an instance of plagiarism
--numWordsPerLine: text reshaped into this number of words per line

Note: this works on a Mac but may not work for another system.
"""

import subprocess as sp
import sys, time
import string


if __name__ == '__main__':

  start = time.time()

  basename, Kstr, m = sys.argv[1:]

  m = int(m)
  K = int(Kstr)
  intext = '../corpus/' + basename + '.txt'

  F = open(intext)
  text=F.read()
  F.close()
  words = text.split()


  numWords = len(words)
  #ith process gets B[round(i*(n-m+1)/K):(round((i+1)*(n-m+1)/K)+m-1)]

  textReshaped = '../corpus/' + basename + 'Reshaped_' + Kstr + 'chunks.txt'
  hashedtext = '../corpus/' + basename + 'Hashed_'  + Kstr + 'chunks.txt'

  # carve/reshape text
  with open(textReshaped,'w') as fout:
      for i in range(K):
          chunk = words[int(round(i*(numWords-m+1)/K)):int(round((i+1)*(numWords-m+1)/K)+m-1)]
          txt = ' '.join([x.translate(string.maketrans("",""), string.punctuation).upper() for x in chunk])
          fout.write(str(i)+': '+txt+'\n')


  ### Hash each line with MR code
  hashCommand = 'python MRhash_word.py < ' + textReshaped + ' > ' + hashedtext
  returnCode = sp.call(hashCommand, shell=True)


  print time.time() - start, 's'
