# -*- coding: utf-8 -*-
"""
Purpose: Take a text file (intext) and reshape so that there's a newline after every 20 words.

usage: python ../cs205final/regroupText.py PrideAndPrejudice.txt PrideAndPrejudiceReshaped.txt

This text should then have file numbers added to it from command line:
  awk '{printf("%5d : %s\n", NR,$0)}' reshapedFile.txt > fileWithNumbers.txt

The file with line numbers should then be hashed with MRhash.py:
  python MRhash.py < fileWithNumbers.txt > hashedFile.txt

"""

#import subprocess as sp
import itertools
import sys

# from http://stackoverflow.com/questions/1624883/alternative-way-to-split-a-list-into-groups-of-n
def grouper(n, iterable, fillvalue=None):
    "grouper(3, 'ABCDEFG', 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return itertools.izip_longest(*args, fillvalue=fillvalue)


if __name__ == '__main__':


  # NAME OF INPUT TEXT, NAME OF TEXT GROUPED INTO 20 WORDS A LINE
  intext, textReshaped = sys.argv[1:]


  #textReshaped = 'FrankensteinReshaped.txt'

  # NAME OF OUTPUT TEXT WITH LINE NUMBERS
  #finaltext = 'FrankensteinWithNum.txt'

  # NAME OF FILE WITH HASHED TEXT
  #hashedtext = 'FrankensteinHashed.txt'



  ### Reshape text (20 words per line)
  F = open(intext)
  text=F.read()
  F.close()
  words = text.split()

  # creates tuples of number of words to group by.
  grouped = grouper(20, words, "")
  text3 = list(grouped)

  fout = open(textReshaped, 'w')
  for i, chunk in enumerate(text3):
      fout.write("%s\n" % (' '.join(chunk)))
  fout.close()


  # Cannot get this to work because of quotes - \n gets interpreted literally at command line
  ### Add line numbers to each line
  #lineNumCommand = '''awk '{printf("%5d : %s\n", NR,$0)}' ''' + textReshaped + ''' > ''' + finaltext
  #lineNumCommand = 'awk '{printf("%5d : %s\n", NR,$0)}' textReshaped > finaltext'
  #print lineNumCommand
  #returnCode = sp.call(lineNumCommand, shell=True)


  ### Hash each line with MR code
  #hashCommand = 'python MRhash.py < ' + finaltext + ' > ' + hashedtext
  #returnCode = sp.call(hashCommand, shell=True)




