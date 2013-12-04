Parallel Rabin-Karp Algorithm
=============================


## CS205 Fall 2013 Final Project

### Project Members:###
* Nikhat Dharani (ndharani@college)
* Pin-Wen Wang (pin-wen_wang@college)

### Introduction: ###

The Rabin-Karp Algorithm is an implementation of exact string matching that uses a rolling hash to find any one set of pattern strings in a text. 

The Rabin-Karp algorithm is used in detecting plagiarism because given a pattern and a source of texts, the algorithm can quickly search through papers for patterns from the source material.

In this project, we implement serial and parallel versions of the Rabin-Karp algorithm, loosely based on the paper: "Parallelized Rabin-Karp Method for Exact String Matching" by Brodanac, P. Budin, L, Jakobovic, D. <http://ieeexplore.ieee.org/xpl/abstractAuthors.jsp?arnumber=5974088>, and we explore different methods of parallelizing the Rabin-Karp algorithm and analyze our findings here: [insert our webpage]

###Approaches: ###
There are a number of approaches to parallelizing the Rabin-Karp algorithm that we will design and analyze. We can divide the corpus of texts, and/or the pattern among processes and run the algorithm on the corpus in real-time (with the rolling hash) or preprocessed. Inspired by the original Rabin-Karp algorithm, we implement the serial and parallel versions as well as attempt our own parallel version using prehashed corpuses.

* Takes the Rabin-Karp algorithm and parallelizes the rolling hash using MPI. 

* Prehash the corpus of texts that will be searched through by hashing each word using MapReduce, and search on the pre-processed corpus of texts for patterns.


### Dependencies: ###

* MapReduce (MRJob)
* MPI (mpi4py)
* Python (sys,string)


### Source Code: ###

* `RabinKarpSerial.py`: Serial implementation of the Rabin-Karp algorithm on a corpus of texts 
* `RabinKarpParallel.py`: MPI-master-slave parallel implementation of the Rabin-Karp algorithm (as described in the paper) on a corpus of texts
* `mpirka.py`: single text MPI-send-recv parallel implementation of the Rabin-Karp algorithm
* `RabinKarpEmbarParallel.py`: MPI-master-slave implementation of running the Rabin-Karp algorithm on a corpus of texts - embarassingly parallel method of sending each processor an entire source text file to run the Rabin-Karp algorithm
* `MRhash_word.py`: Prehashes a source text by word using MapReduce and divides the hashed text into X chunks 
* `mpiRK_chunkCorpus.py`: single text parallel implementation of Rabin-Karp algorithm where each process searches for pattern in one chunk of prehashed corpus text.
* `MasterSlave_chunkCorpus.py`: prehashed corpus (single text) is chunked into small pieces and dynamically distributed to slaves.
* `MasterSlave_chunkMultCorpus.py`: prehashed corpus (multiple texts) is chunked into small pieces and dynamically distributed to slaves.
* `regroupText2.py`: Reshapes text into X pieces and hashes them using MRhash_word.py

### Acknowledgements: ###
* Louis Mullie: Project Mentor
* Cris Cecka: Course instructor
