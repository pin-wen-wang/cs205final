#
#usage: python mpirka2.py [single text] [pattern]
#
from mpi4py import MPI
import numpy as np
import sys
import string

# set up communication world
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


d = 26

def splitCount(s, count):
	return[s[i:i+count] for i in range(0,len(s),count)]

def prep_text(text):
	exclude = set(string.punctuation)
	return ''.join(x.upper() for x in text if x not in exclude)	

def sub_search(txt,pat,q):
	#print "txt from rank %d: %s" %(rank,txt)
	#print "pat from rank %d: %s" %(rank,pat)
	txtlen = len(txt)
	patlen = len(pat)
	hashpat = 0
	hashtxt = 0
	h = 1
	#assert txtlen > patlen

	for i in range(0,patlen-1):
		#  print "creating h\n"
		  h = (h*d)%q

	for i in range(0,patlen):
		#  print "creating hash values\n"
		  hashpat = (d*hashpat + ord(pat[i]))%q
		  hashtxt = (d*hashtxt + ord(txt[i]))%q

	match_list = []
	for i in range(0,txtlen-patlen+1):
		#  print "running through txt\n"
		  # check all the letters if the hashes match
		  if (hashpat == hashtxt):
		#	  match_list.append((i,txt[i:i+patlen]))
	 	# 	  comm.send(txt[i:i+patlen],dest=0,tag=i)
		#	  print "hashes are equal\n"
		#	  double hashin
		#	if(hashpat2==hashtxt2):
		#		match_list.append((i,txt[i:i+patlen]))
		#		print "pattern found at index %d" %i
		#		print "pattern %s" %txt[i:i+patlen]
			  for j in range (0,patlen):
				if (txt[i+j] != pat[j]):
					break
			 	if j == patlen-1:
					match_list.append((i,txt[i:i+patlen]))

		  if (i < txtlen-patlen):
		#   print "shifting pat in txt\n"
		  	hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
		  	if (hashtxt < 0):
				hashtxt = hashtxt + q

 	comm.send(match_list,dest=0)

def full_search(txt,pat,q,patsize):

	splitpat=splitCount(pat,patsize)
	
	for subpat in range(0,len(splitpat)):
		sub_search(txt,splitpat[subpat],q)


# distribute data to other processes to do computations
searchtxt, pattxt = sys.argv[1:]
with open (pattxt,"r") as patfile:
	pat=patfile.read().replace('\n',' ')

pat = prep_text(pat) 
patlen = len(pat)
#print "patlen %d" %patlen

if rank == 0:

	with open (searchtxt,"r") as txt:
		txt = txt.read().replace('\n',' ')
		
	txt = prep_text(txt) 

	txtlen = len(txt)
	#print "txtlen %d" %txtlen

	local_data = txt[0:int(round(1*(txtlen-patlen+1)/size)+(patlen-1))]

	for i in xrange(1,size):
		start = int(round((i*(txtlen-patlen+1)/size)))
		end = int(round((i+1)*(txtlen-patlen+1)/size)+(patlen-1))
		#print start
		#print end
		if end > txtlen :
			end = txtlen
		send_data = txt[start:end]
		#need to keep track of start so we know the absolute index
		comm.send(send_data,dest=i)
else :
	local_data = comm.recv(source=0)


q = 1079
patsize = 100

full_search(local_data,pat,q,patsize)

#sub_search(local_data,pat,q)

# 0 processes all the results
if rank == 0:

	status = MPI.Status()
	
	for i in xrange(1,size):
		recv_result = comm.recv(source=i,tag=MPI.ANY_TAG)
		start = int(round((i*(txtlen-patlen+1)/size)))

	  	for index, match in recv_result:
		       	abs_index = start+index
	       		print "pattern found at index %d" %(abs_index)
	       		print "pattern: %s" %match
	       		#calculate absolute index

