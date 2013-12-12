##################################################################################
# embarassingly parallel master-slave Rabin Karp algorithm
# implemented from paper
# usage : mpiexec -n [# of processors] python filenames.txt multipattern.txt
##################################################################################

from mpi4py import MPI
import numpy as np
import sys
import string

# set up communication world
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

#d value for rolling hash
d = 26

#splits pattern into specified pattern size before searching
def splitCount(s, count):
	return[s[i:i+count] for i in range(0,len(s),count)]

# removes punctuation and capitalizes letters to prep for processing
def prep_text(text):
	exclude = set(string.punctuation)
	return ''.join(x.upper() for x in text if x not in exclude)	

#runs the rka of a piece of the pattern on piece of text
def sub_search(txt,pat,q,matchlist):
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
					matchlist.append((i,txt[i:i+patlen]))
			      	#	print "pattern found at index %d from file: %s" %(i,name)
			       	#	print "pattern: %s" %txt[i:i+patlen]

		  if (i < txtlen-patlen):
		#   print "shifting pat in txt\n"
		  	hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
		  	if (hashtxt < 0):
				hashtxt = hashtxt + q

#runs the rka of a piece of the pattern on piece of text
def full_search(txt,pat,q,patsize,name):

	splitpat=splitCount(pat,patsize)
	matchlist = []	
	for subpat in range(0,len(splitpat)):
		sub_search(txt,splitpat[subpat],q,matchlist)

	if len(matchlist) == 0:
		results = matchlist
	else: 
		results = post_process(patsize,matchlist)

	for index,match in results:
		
		print "pattern found at index %d in text: %s" %(index,name)
		print "pattern: %s" %match

	#tell master that it's finished and needs another file
	comm.send(1,dest=0)

# combines consecutive matches for entire match
def post_process(patlen,recv_result):

# combines consecutive matches for entire match
	match_len = len(recv_result)
	result = []
	curr = 0
	offset = 1
	
	if match_len == 1:
		result.append(recv_result[curr])
		return result
	else:
		index,string = recv_result[curr]

	while  curr < match_len:
		nextcurr = curr+offset
	
		if nextcurr < match_len and index + offset*patlen == recv_result[nextcurr][0] :
			string = string + " " + recv_result[nextcurr][1]
			offset += 1			
		else:
			result.append((index,string))
			curr = nextcurr 
			if curr < match_len:
				index,string = recv_result[curr]	

	return result

# divides files into equal pieces for each slave processor and sends that part of each 
# file to processors until no more files need to be processed and calculates absolute 
# index once each file returns its matches
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

	#if there aren't enough txtfiles to give to each processor
	assert numfiles >= size-1
	
	#initialization of first file in all processors
	#print 'txtlen %d' %txtlen

	for i in xrange(0,size-1):
		#need to keep track of start so we know the absolute index
		comm.send(text_list[i],dest=i+1)

	count = size-2

	
	while received < total-1:
	
		recv_result = comm.recv(source=MPI.ANY_SOURCE,tag=MPI.ANY_TAG,status=status)
		slave = status.Get_source() 
			#print 'filecount received from slave %d: %d' %(i,filecount)
		#update total number of received parts
		received += 1
	
		#update to next file to process
		count += 1
		#send the next file part to processor when it's finished	
		#each slave must do a part in each file
		if count < numfiles:
			comm.send(text_list[count],dest=slave)
			
	#stop all processes when everyone is done
	for s in range(1,size):
		comm.send(-1,dest=s,tag=100)			

#conducts the rka on its piece of the text
def slave(pat,q,patlen):
	
	status = MPI.Status()

	while True:
		local_data = comm.recv(source=0,tag=MPI.ANY_TAG,status=status)
		if local_data == -1: break
		name,txt = local_data
		#print 'filecount in slave %d: %d' %(rank,currfile)
		#break out of the while loop if all processes are done
		full_search(txt,pat,q,patlen,name)


###############################----MAIN----#############################################

if __name__ == '__main__':

	if len(argv) != 3:
		print "Usage: mpiexec -n [# of processors] python", argv[0], "[corpus filenames] [input text]"
		exit()
	# distribute data to other processes to do computations
	filenames, pattxt = argv[1:]
	with open (pattxt,"r") as patfile:
		pat=patfile.read().replace('\n',' ')
	
	pat = prep_text(pat) 

	#size of pattern we want to match
	patsize = 100 
	
	q = 1079

	start = MPI.Wtime()
	if rank == 0:
		master(filenames,patsize)
	else:
		slave(pat,q,patsize)

	end = MPI.Wtime()
	print "Time: %f sec" %(end-start)
