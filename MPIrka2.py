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

def sub_search(txt,pat,q,name):
	#print pat
#	print "txt from rank %d " %(rank)
	txtlen = len(txt)
	patlen = len(pat)
	hashpat = 0
	hashtxt = 0
	h = 1

	assert txtlen > patlen

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
		#	  match_list.append((i,txt[i:i+patlen]))
		#	  if(hashpat2==hashtxt2):
		#		match_list.append((i,txt[i:i+patlen]))
		#		print "pattern found at index %d" %i
		#		print "pattern %s" %txt[i:i+patlen]
			  for j in range (0,patlen):
				if (txt[i+j] != pat[j]):
					break
			 	if j == patlen-1:
			      		print "pattern found at index %d from file: %s" %(i,name)
			       		print "pattern: %s" %txt[i:i+patlen]

		  if (i < txtlen-patlen):
		#   print "shifting pat in txt\n"
		  	hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
		  	if (hashtxt < 0):
				hashtxt = hashtxt + q

def full_search(txt,pat,q,patsize,name):

	splitpat=splitCount(pat,patsize)
	matchlist = []	
	for subpat in range(0,len(splitpat)):
		sub_search(txt,splitpat[subpat],q,name)

	#tell master that it's finished and needs another file
	comm.send(1,dest=0)

def master(filenames,patlen):

	status = MPI.Status()

	text_list = []
	files = open(filenames).readlines()
	for i in files:
		filename = i.replace('\n','')

		with open (filename,"r") as txt:
			txt = txt.read().replace('\n',' ')
		
		text_list.append((filename,prep_text(txt)))

		#print "txtlen %d" %txtlen
	

	#embarassingly parallel test
	#use master slave to give each processor a full text to run rka on
	#see how timing compares
	numfiles = len(text_list)
	#print 'numfiles: %d' %(numfiles)

	#counter of received processed file parts per processor
	received = 0 
	total = numfiles
	print 'total %d' %(total)

	#if there aren't enough txtfiles to give to each processor
	assert numfiles > size
	
	#initialization of first file in all processors
	#print 'txtlen %d' %txtlen

	for i in xrange(0,size-1):
		#need to keep track of start so we know the absolute index
		comm.send(text_list[i],dest=i+1)

	count = (size-1)

	
	while received < total-1:
	
		recv_result = comm.recv(source=MPI.ANY_SOURCE,tag=MPI.ANY_TAG,status=status)
		slave = status.Get_source() 
		print 'slave: %d' %slave
			#print 'filecount received from slave %d: %d' %(i,filecount)
		#update total number of received parts
		received += 1
		print 'received %d' %received
	
		#update to next file to process
		count += 1
		#send the next file part to processor when it's finished	
		#each slave must do a part in each file
		if count < numfiles:
			print 'count: %d' %count
			comm.send(text_list[count],dest=slave)
			
	print 'out of loop'
	#stop all processes when everyone is done
	for s in range(1,size):
		comm.send(-1,dest=s,tag=100)			

def slave(pat,q,patlen):
	
	status = MPI.Status()

	while True:
		local_data = comm.recv(source=0,tag=MPI.ANY_TAG,status=status)
		if local_data == -1: break
		name,txt = local_data
		#print 'filecount in slave %d: %d' %(rank,currfile)
		#break out of the while loop if all processes are done
		full_search(txt,pat,q,patlen,name)


if __name__ == '__main__':

	# distribute data to other processes to do computations
	filenames, pattxt = sys.argv[1:]
	with open (pattxt,"r") as patfile:
		pat=patfile.read().replace('\n',' ')
	
	pat = prep_text(pat) 

	#size of pattern we want to match
	patsize = 100 
	
	q = 1079

	if rank == 0:
		start = MPI.Wtime()
		master(filenames,patsize)
		end = MPI.Wtime()
		print "Time: %f sec" %(end-start)
	else:
		slave(pat,q,patsize)

