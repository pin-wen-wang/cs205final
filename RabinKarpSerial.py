#########################################
# http://www.geeksforgeeks.org/searching-for-patterns-set-3-rabin-karp-algorithm#/
# serial Rabin Karp algorithm
#########################################

d = 256 # number of characters in input alphabet?

def search(txt,pat,q):
  patlen = len(pat)
  txtlen = len(txt)
  hashpat = 0
  hashtxt = 0
  h = 1

  for i in range(0,patlen-1):
	  print "creating h\n"
	  h = (h*d)%q
  for i in range(0,patlen):
	  print "creating hash values\n"
	  hashpat = (d*hashpat + ord(pat[i]))%q
	  hashtxt = (d*hashtxt + ord(txt[i]))%q
  
  for i in range(0,txtlen-patlen+1):
	  print "running through txt\n"
	  # check all the letters if the hashes match
	  if (hashpat == hashtxt):
		  print "hashes are equal\n"
		  for j in range (0,patlen):
			  if (txt[i+j] != pat[j]):
				break
		  	  if j == patlen-1:
			  	print "pattern found at index %d" %i 
				return

	  if (i < txtlen-patlen):
		print "shifting pat in txt\n"
	  	hashtxt = (d*(hashtxt - ord(txt[i])*h) + ord(txt[i+patlen]))%q
		if (hashtxt < 0):
			hashtxt = hashtxt + q

if __name__ == '__main__':

  txt = "my name is bob"
  pat = "name is"
  q = 101
  search(txt,pat,q)
