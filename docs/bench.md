## Bench

To make a bench between framework, you must identify three step:
- Time to *start the local cluster*, if any.
- Time to compile the first time, the python code to C or GPU
- Time to run the performance tests

Our recommandation it to run one time your test, and only after, run multiple time the
same performance tests and calculate the
[timeit](https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-timeit)
magic command. The Python 3.11 need the same approach.
