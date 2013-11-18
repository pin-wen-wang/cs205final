from mpi4py import MPI
import numpy as np
import sys

# set up communication world
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()


d = 26

def splitCount(s, count):
	return[s[i:i+count] for i in range(0,len(s),count)]

def sub_search(txt,pat,q):
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

	for i in range(0,txtlen-patlen+1):
		#  print "running through txt\n"
		  # check all the letters if the hashes match
		  if (hashpat == hashtxt):
			 comm.send(txt[i:i+patlen],dest=0,tag=i)
		#	  print "hashes are equal\n"
		#	  double hashin
		#	if(hashpat2==hashtxt2):
		#		tuple_array.append((i,txt[i:i+patlen]))
		#		print "pattern found at index %d" %i
		#		print "pattern %s" %txt[i:i+patlen]
			#  for j in range (0,patlen):
			#	  if (txt[i+j] != pat[j]):
			#		break
			 #	  if j == patlen-1:
			 #	  	comm.send(txt[i:i+patlen],dest=0,tag=i)

		  if (i < txtlen-patlen):
		#   print "shifting pat in txt\n"
		    hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
		    if (hashtxt < 0):
			hashtxt = hashtxt + q


# distribute data to other processes to do computations
searchtxt, pattxt = sys.argv[1:]
with open (pattxt,"r") as patfile:
	pat=patfile.read().replace('\n',' ')

pat = pat.upper()

if rank == 0:

	searchtxt, pattxt = sys.argv[1:]
	with open (searchtxt,"r") as txt:
		txt = txt.read().replace('\n',' ')
		
	txt = txt.upper()

	txtlen = len(txt)
	patlen = len(pat)

	local_data = txt[0:int(round(1*(txtlen-patlen+1)/size)+(patlen-1))]
	for i in xrange(1,size):
		start = int(round((i*(txtlen-patlen+1)/size)))
		end = int(round((i+1)*(txtlen-patlen+1)/size)+(patlen-1))
		print start
		print end
		if end >= txtlen :
			end = txtlen-1
		send_data = txt[start:end]
		#need to keep track of start so we know the absolute index
		comm.send(send_data,dest=i)
else :
	local_data = comm.recv(source=0)


q = 1079
patsize = 100

sub_search(local_data,pat,q)

# 0 processes all the results
if rank == 0:

	status = MPI.Status()
	for i in xrange(1,size):
		recv_result = comm.recv(source=i,tag=MPI.ANY_TAG)
		start = int(round((i*(txtlen-patlen+1)/size)))
		index = status.Get_tag()
		abs_index = start+index
		print "pattern found at index %d" %(abs_index)
		print "pattern: %s" %recv_result
		#calculate absolute index

