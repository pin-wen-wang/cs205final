# -*- coding: utf-8 -*-
"""
Purpose: Take a text file (intext) and reshape so that there's a newline after every x words or divide into K (#processes) pieces. Add line numbers with awk command if needed. Call the appropriate MR code to hash the file.

usage: python regroupText.py ../corpus/Frankenstein.txt 0 4 20 40

Input
--intext: name of input text to be reshaped.
--mode: whether to chop corpus text into K pieces (mpi) or by numWordsPerLine words (for master/slave implementation with mpi)
--K: number of processes.
--m: number of matched, consecutive words to constitute an instance of plagiarism
--numWordsPerLine: text reshaped into this number of words per line

NOTE: mode = 0 isn't done because there's no overlap yet. (0-39;40-79 can't find 35-40 for m=6)
Note: this works on a Mac but may not work for another system.
"""

import subprocess as sp
import itertools
import sys, time
import numpy as np
import string

# from http://stackoverflow.com/questions/1624883/alternative-way-to-split-a-list-into-groups-of-n
def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


if __name__ == '__main__':

  start = time.time()

  basename, mode, Kstr, m, numWordsPerLine = sys.argv[1:]

  m = int(m)
  K = int(Kstr)
  intext = '../corpus/' + basename + '.txt' # without .txt

  F = open(intext)
  text=F.read()
  F.close()
  words = text.split()


  if int(mode):

    numWords = len(words)
    numPerProcess = int(np.ceil(numWords/float(K)))
    #ith process gets B[round(i*(n-m+1)/K):(round((i+1)*(n-m+1)/K)+m-1)]

    textReshaped = '../oldCode/' + basename + 'Reshaped_' + Kstr + 'mpi.txt'
    #textWithNum = '../oldCode/' + basename + 'WithNum_'  + Kstr + 'mpi.txt'
    hashedtext = '../oldCode/' + basename + 'Hashed_'  + Kstr + 'mpi.txt'

    with open(textReshaped,'w') as fout:
        for i in range(K):
            chunk = words[int(round(i*(numWords-m+1)/K)):int(round((i+1)*(numWords-m+1)/K)+m-1)]
            txt = ' '.join([x.translate(string.maketrans("",""), string.punctuation).upper() for x in chunk])
            fout.write(str(i)+': '+txt+'\n')

    ### Hash each line with MR code
    hashCommand = 'python MRhash_word.py < ' + textReshaped + ' > ' + hashedtext
    returnCode = sp.call(hashCommand, shell=True)


  else:

    # creates tuples of number of words to group by.
    grouped = grouper(int(numWordsPerLine), words, "")
    text3 = list(grouped)

    textReshaped = '../oldCode/' + basename + 'Reshaped_' + numWordsPerLine + 'mslave.txt'
    textWithNum = '../oldCode/' + basename + 'WithNum_' + numWordsPerLine + 'mslave.txt'
    hashedtext = '../oldCode/' + basename + 'Hashed_' + numWordsPerLine + 'mslave.txt'

    fout = open(textReshaped, 'w')
    for i, chunk in enumerate(text3):
        fout.write("%s\n" % (' '.join(chunk)))
    fout.close()

    ### Add line numbers to each line
    lineNumCommand = r'''awk '{printf("%5d : %s\n", NR,$0)}' ''' + textReshaped + ' > ' + textWithNum
    returnCode = sp.call(lineNumCommand, shell=True)

    ### Hash each line with MR code
    hashCommand = 'python MRhash.py < ' + textWithNum + ' > ' + hashedtext
    returnCode = sp.call(hashCommand, shell=True)


  print time.time() - start, 's'
