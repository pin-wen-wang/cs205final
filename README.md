Parallel Rabin-Karp Algorithm
=============================


## CS205 Fall 2013 Final Project

### Project Members:###
* Nikhat Dharani (ndharani@college)
* Pin-Wen Wang (pin-wen\_wang@college)

### Introduction: ###

The Rabin-Karp Algorithm is an implementation of exact string matching that uses a rolling hash to find any one set of pattern strings in a text.

The Rabin-Karp algorithm is used in detecting plagiarism because given a pattern and a source of texts, the algorithm can quickly search through papers for patterns from the source material.

In this project, we implement serial and parallel versions of the Rabin-Karp algorithm, loosely based on the paper: "Parallelized Rabin-Karp Method for Exact String Matching" by Brodanac, P. Budin, L, Jakobovic, D. <http://ieeexplore.ieee.org/xpl/abstractAuthors.jsp?arnumber=5974088>, and we explore different methods of parallelizing the Rabin-Karp algorithm and analyze our findings here.

###Approaches: ###
There are a number of approaches to parallelizing the Rabin-Karp algorithm that we will design and analyze. We can divide the corpus of texts, and/or the pattern among processes and run the algorithm on the corpus in real-time (with the rolling hash) or preprocessed. Inspired by the original Rabin-Karp algorithm, we implement the serial and parallel versions as well as attempt our own parallel version using prehashed corpuses.

* Takes the Rabin-Karp algorithm and parallelizes the rolling hash using MPI.

* Prehash the corpus of texts that will be searched through by hashing each word using MapReduce, and search on the pre-processed corpus of texts for patterns.


### Dependencies: ###

* MapReduce (MRJob)
* MPI (mpi4py)
* Python (sys, string, numpy, itertools, pandas, subprocess, time, operator)

The above packages should already be installed on the resonance.seas cluster for CS205. On the resonance node, `module load courses/cs205/2013` will load the modules automatically.

### Source Code: ###

* `RabinKarpSerial.py`: Serial implementation of the Rabin-Karp algorithm on a corpus of texts
* `RabinKarpParallel.py`: MPI-master-slave parallel implementation of the Rabin-Karp algorithm (as described in the paper) on a corpus of texts
* `mpirka.py`: single text MPI-send-recv parallel implementation of the Rabin-Karp algorithm
* `RabinKarpEmbarParallel.py`: MPI-master-slave implementation of running the Rabin-Karp algorithm on a corpus of texts - embarassingly parallel method of sending each processor an entire source text file to run the Rabin-Karp algorithm
* `MRhash_word.py`: Prehashes a source text by word using MapReduce and divides the hashed text into X chunks
* `MRhash.py`: Obsolete mapper/reducer. This file is used because a function defined in it (letsHash) is still relevant to other code.
* `mpiRK_chunkCorpus.py`: [prehashed corpus] single text parallel implementation of Rabin-Karp algorithm where each process searches for pattern in one chunk of prehashed corpus text.
* `mpiRK_chunkMultCorpus.py`: [prehashed corpus] multiple texts parallel implementation of Rabin-Karp algorithm where each process searches for pattern in one chunk of prehashed corpus texts.
* `MasterSlave_chunkCorpus.py`: [prehashed corpus] single text is chunked into small pieces and dynamically distributed to slaves.
* `MasterSlave_chunkMultCorpus.py`: [prehashed corpus] multiple texts are individually chunked into small pieces and dynamically distributed to slaves.
* `regroupText2.py`: Reshapes text into K pieces and hashes text using MRhash\_word.py, assuming m identical words=plagiarism. Note: makes a command line call. Example: python regroupText2.py PrideAndPrejudice 20 20

### Usage: ###
Inside the directory `Hash-by-Char` you can run the serial and parallel code by running the following:
* RabinKarpSerial.py: serial version that hashes in real time
`Usage: python RabinKarpSerial.py [file w/ corpus text names] [pattern text]`
`Example: python RabinKarpSerial.py filenames.txt multipattern.txt`

* RabinKarpParallel.py: parallel version that hashes in real time
`Usage: mpiexec -n [# of processes] python RabinKarpParallel.py 
	[file w/ corpus text names] [pattern text]`
`Example: mpiexec -n 20 python 
	RabinKarpParallel.py filenames.txt multipattern.txt`


Inside the directory `Hash-by-Word` you can run the serial and parallel code by running the following:
* prehashedSerialRK.py: serial version using prehashed corpus
`Usage: python prehashedSerialRK.py 
	[text file hashed w/ regroupText2.py] [pattern file (unhashed)]`
`Example: python prehashedSerialRK.py 
	txt/english\_smallerHashed\_2000chunks.txt multipattern50.txt`

* MasterSlave\_chunkCorpus.py: parallel version w/ master/slave framework using prehashed corpus
`Usage: mpiexec -n [# of processes] python MasterSlave_chunkCorpus.py 
	[text file hashed w/ regroupText2.py] [pattern file (unhashed)]`
`Example: mpiexec -n 20 python MasterSlave_chunkCorpus.py 
	txt/english_smallerHashed_2000chunks.txt multipattern50.txt`

* MasterSlave\_chunkMultCorpus.py: master/slave framework using prehashed corpus w/ multiple texts (filenames)
`Usage: mpiexec -n [# of processes] python MasterSlave_chunkMultCorpus.py 
	[file w/ list of hashed files] [pattern file (unhashed)]`
`Example: mpiexec -n 20 python MasterSlave_chunkMultCorpus.py 
	txt/hashedfilenames.txt multipattern50.txt`

* mpiRK\_chunkCorpus.py: mpi parallel version w/ #chunks of corpus = #processes, using prehashed corpus

`Usage: mpiexec -n [# of processes] python mpiRK_chunkCorpus.py 
	[text file hashed w/ regroupText2.py] [pattern file (unhashed)]`
`Example: mpiexec -n 20 python mpiRK_chunkCorpus.py 
	txt/english_smallerHashed_20chunks.txt multipattern50.txt`

* mpiRK\_chunkMultCorpus.py: mpi w/ #chunks of corpus w/  multiple texts (filenames)
`Usage: mpiexec -n [# of processes] python mpiRK_chunkMultCorpus.py 
	[file w/ list of hashed files] [pattern file (unhashed)]`
`Example: mpiexec -n 20 python mpiRK_chunkMultCorpus.py 
	txt/hashedfilenames.txt multipattern50.txt`

* `regroupText2.py`: reshape and hash corpus text. text must be in txt folder.
`Usage: python regroupText2.py [filename w/out '.txt'] [# of chunks] [# of words constituting plagiarism]`
`Example: python regroupText2.py PrideAndPrejudice 20 20`

### Acknowledgements: ###
* Louis Mullie: Project Mentor
* Cris Cecka: Course instructor
