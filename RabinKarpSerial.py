#########################################
# http://www.geeksforgeeks.org/searching-for-patterns-set-3-rabin-karp-algorithm#/
# serial Rabin Karp algorithm
# currently runs by : python RabinKarpSerial.py filenames.txt pattern2.txt
#########################################
import sys
d = 26 # number of characters in input alphabet?

def splitCount(s, count):
	return[s[i:i+count] for i in range(0,len(s),count)]

def sub_search(txt,pat,q,filename):

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
	  hashpat = (d*hashpat + ord(pat[i]))%q
          hashpat2 = (d*hashpat2 + ord(pat[i]))%q2
	  hashtxt = (d*hashtxt + ord(txt[i]))%q
          hashtxt2 = (d*hashtxt2 + ord(txt[i]))%q2

  for i in range(0,txtlen-patlen+1):
	#  print "running through txt\n"
	  # check all the letters if the hashes match
	  if (hashpat == hashtxt):
	#	  print "hashes are equal\n"
	#	  double hashin
	#	if(hashpat2==hashtxt2):
	#		tuple_array.append((i,txt[i:i+patlen]))
	#		print "pattern found at index %d" %i
	#		print "pattern %s" %txt[i:i+patlen]
		  for j in range (0,patlen):
			  if (txt[i+j] != pat[j]):
				break
		 	  if j == patlen-1:
			  	print "pattern found at index %d in text: %s" %(i,filename)
				print "pattern: %s" %txt[i:i+patlen]
		#		return

	  if (i < txtlen-patlen):
	#   print "shifting pat in txt\n"
	    hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
	    hashtxt2 = (d*(hashtxt2 - ord(txt[i])*h2) + ord(txt[i+patlen]))%q2
	    if (hashtxt < 0):
		hashtxt = hashtxt + q
	    if (hashtxt2 < 0):
		hashtxt2 = hashtxt2 + q2

def full_search(txt,pat,q,patsize,filename):

  splitpat = splitCount(pat,patsize)

  for subpat in range(0,len(splitpat)):
	  sub_search(txt,splitpat[subpat],q,filename)

if __name__ == '__main__':

	q = 1079
	q2 = 1011
	patsize = 100;
	
	# opening many files

	filenames, pattxt = sys.argv[1:]
	with open (pattxt,"r") as patfile:
		pat=patfile.read().replace('\n',' ')

	pat = pat.upper()

	files = open(filenames).readlines()
	for i in files:
		filename = i.replace('\n','')
		with open (filename,"r") as txt:
			txt = txt.read().replace('\n',' ')
		
		txt = txt.upper()gi

		full_search(txt,pat,q,patsize,filename)

