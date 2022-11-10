## Bench

To make a bench between framework, you must identify three step:
- Time to *start local to cluster*, if any.
- Time to compile the first time, the python code to C ou GPU
- Time to run the performance tests

Your recommandation it to run one time your test, and only after, run multiple time the
same performance tests and calculate the
[timeit](https://ipython.readthedocs.io/en/stable/interactive/magics.html#magic-timeit)
magic command.

