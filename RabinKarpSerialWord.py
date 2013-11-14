#########################################
# http://www.geeksforgeeks.org/searching-for-patterns-set-3-rabin-karp-algorithm#/
# serial Rabin Karp algorithm
#########################################
import sys
import string
from itertools import count, groupby
d = 26 # number of characters in input alphabet?

def preprocesspat(pat):
	exclude = set(string.punctuation)
	patarray = pat.split()
	for index,word in enumerate(patarray):
		patarray[index] = ''.join(ch.upper() for ch in word if ch not in exclude)
	return patarray

def preprocesstxt(txt):
	exclude = set(string.punctuation)
	return ''.join(ch.upper() for ch in txt if txt not in exclude)

def splitCount(s,step):
	c=count()
	return[' '.join(g) for k, g in groupby(s, lambda i: c.next() // step) ]

def word_hash(word,q):
	x = 0;
	for i in range(len(word)):
	  x = (d*x + ord(word[i]))%q
	return x 

def sub_search(txt,pat,q):

  patlen = len(pat)
  txtlen = len(txt)
  hashpat = 0
  hashpat2 = 0
  hashtxt = 0
  hashtxt2 = 0
  h = 1
  h2 = 1
  tuple_array = []

  for i in range(0,patlen-1):
	#  print "creating h\n"
	  h = (h*d)%q
  	  h2 = (h2*d)%q2
  for i in range(0,patlen):
	#  print "creating hash values\n"
	  hashpat = (d*hashpat + word_hash(pat[i],q))%q
          hashpat2 = (d*hashpat2 + word_hash(pat[i],q2))%q2
	  hashtxt = (d*hashtxt + word_hash(txt[i],q))%q
          hashtxt2 = (d*hashtxt2 + word_hash(txt[i],q2))%q2

  for i in range(0,txtlen-patlen+1):
	#  print "running through txt\n"
	  # check all the letters if the hashes match
	  if (hashpat == hashtxt):
	#	  print "hashes are equal\n"
	#	  double hashing
	#	if(hashpat2==hashtxt2):
	#		tuple_array.append((i,txt[i:i+patlen]))
	#		print "pattern found at index %d" %i
	#		print "pattern %s" %txt[i:i+patlen]
		  for j in range (0,patlen):
			  if (txt[i+j] != pat[j]):
				break
		 	  if j == patlen-1:
			  	print "pattern found at index %d" %i
				print "pattern: %s" %txt[i:i+patlen]
		#		return

	  if (i < txtlen-patlen):
	#   print "shifting pat in txt\n"
	    hashtxt = (d*(hashtxt - word_hash(txt[i],q2)*h) + word_hash(txt[i+patlen],q))%q
	    hashtxt2 = (d*(hashtxt2 - word_hash(txt[i],q2)*h2) + word_hash(txt[i+patlen],q2))%q2
	    if (hashtxt < 0):
		hashtxt = hashtxt + q
	    if (hashtxt2 < 0):
		hashtxt2 = hashtxt2 + q2 


def full_search(txt,pat,q,patsize):

  splitpat = splitCount(pat,patsize)

  for subpat in range(0,len(splitpat)):
	  sub_search(txt,splitpat[subpat],q)

if __name__ == '__main__':

  datatxt, pattxt = sys.argv[1:]
  with open (datatxt, "r") as txtfile:
	  txt=txtfile.read().replace('\n', ' ') # replace newline with space
  with open (pattxt,"r") as patfile:
	  pat=patfile.read().replace('\n',' ')

  txt = preprocesstxt(txt)
  pat = preprocesspat(pat)
  
  #txt = "my name is bob"
  #pat = "name is"
  q = 1079
  q2 = 1011
  patsize = 20; #20 words

  full_search(txt,pat,q,patsize)
