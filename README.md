cs205final
==========

cs205 final project parallel plagiarism

Take the Rabin-Karp string search algorithm that goes character by character and parallelize it using MPI. 

Prehash the corpus of texts that will be searched through by hashing each word using MapReduce.

Divide corpus and pattern among processes to build an efficient parallelized, Rabin-Karp inspired algorithm. 
