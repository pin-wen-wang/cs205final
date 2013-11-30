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
					matchlist.append((i,txt[i:i+patlen]))

		  if (i < txtlen-patlen):
		#   print "shifting pat in txt\n"
		  	hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
		  	if (hashtxt < 0):
				hashtxt = hashtxt + q

def full_search(txt,pat,q,patsize,filecount):

	splitpat=splitCount(pat,patsize)
	matchlist = []	
	for subpat in range(0,len(splitpat)):
		sub_search(txt,splitpat[subpat],q,matchlist)

 	comm.send(matchlist,dest=0,tag=filecount)

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
	

	# instead of dividing the file into the number of processors,
	# divide the file into a set number of pieces and keep sending
	# to processors until there are no more pieces from the files to 
	# run
	numfiles = len(text_list)
	k = size-1
	#print 'numfiles: %d' %(numfiles)

	#keeps a count of which file the slave processor is on and if there are any left 
	#for it to process
	count = [0]*k

	#counter of received processed file parts per processor
	received = 0 
	total = numfiles*k
	#print 'total %d' %(total)
	
	filecount = 0
	
	#initialization of first file in all processors
	name,txt = text_list[0] 
	txtlen = len(txt)
	#print 'txtlen %d' %txtlen

	for i in xrange(1,size):
		start = int(round((i-1)*(txtlen-patlen+1)/k))
		end = int(round(i*(txtlen-patlen+1)/k)+(patlen-1))
	#	print 'start %d' %start
	#	print 'end %d' %end
		send_data = txt[start:end]
		#need to keep track of start so we know the absolute index
		comm.send(send_data,dest=i,tag=filecount)

	
	while received < total:
	
		for i in xrange(1,size):
			recv_result = comm.recv(source=i,tag=MPI.ANY_TAG,status=status)
			slave = i 
			#print 'slave: %d' %slave
			#print 'filecount received from slave %d: %d' %(i,filecount)
			name,txt = text_list[filecount]
			txtlen = len(txt)
			start = int(round(((slave-1)*(txtlen-patlen+1)/k)))

			#post processing for combining consecutive matches
			result = []
			match_len = len(recv_result)
			if match_len > 0 :
				result = post_process(patlen,recv_result) 
			else:
				result = recv_result

			#print out results received from that slave
			for index, match in result:
			       	abs_index = start+index
			      	print "pattern found at index %d from file: %s" %(abs_index,name)
			       	print "pattern: %s" %match
			       	#calculate absolute index
			
			#update total number of received parts
			received += 1
			#print 'received %d' %received
	
			#update to next file to process
			currfile = filecount+1
			#send the next file part to processor when it's finished	
			#each slave must do a part in each file
			if currfile < numfiles:
				
				#print 'filecount sending out to slave %d: %d' %(i,currfile)
				name,txt = text_list[currfile] 
				txtlen = len(txt)
				#print 'txtlen: %d' %txtlen
				start = int(round(((slave-1)*(txtlen-patlen+1))/k))
				end = int(round(slave*(txtlen-patlen+1)/k)+(patlen-1))
				send_data = txt[start:end]
				comm.send(send_data,dest=slave,tag=currfile)
				
		filecount += 1
	
	#stop all processes when everyone is done
	for s in range(1,size):
		comm.send(-1,dest=s,tag=100)			



def slave(pat,q,patlen):
	
	status = MPI.Status()

	while True:
		local_data = comm.recv(source=0,tag=MPI.ANY_TAG,status=status)
		currfile = status.Get_tag()
		#print 'filecount in slave %d: %d' %(rank,currfile)
		#break out of the while loop if all processes are done
		if local_data == -1: break
		full_search(local_data,pat,q,patlen,currfile)


if __name__ == '__main__':

	# distribute data to other processes to do computations
	filenames, pattxt = sys.argv[1:]
	with open (pattxt,"r") as patfile:
		pat=patfile.read().replace('\n',' ')
	
	pat = prep_text(pat) 

	#size of pattern we want to match
	patsize = 50 
	
	q = 1079

	if rank == 0:
		start = MPI.Wtime()
		master(filenames,patsize)
		end = MPI.Wtime()
		print "Time: %f sec" %(end-start)
	else:
		slave(pat,q,patsize)

