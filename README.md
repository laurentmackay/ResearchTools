# ResearchTools

A package for various tools that can be useful in scientific research projects. Two design principles drive this project.

1. Heavy emphasis on using functions/closures, procedures are typically more straightforward to re-purpose than Classes/Objects.
	- Closures typically serve a single purpose, and may expose 1-2 other fields for manipulating internal variables.
		- Keep it Simple and Modular, memory is in no shortage these days but CPU cycles are always costly.
	- Numpy ND-arrays for anything that will be used for number-crunching.
	- When more structure is needed, we use the base Python datatypes as much as possible.
		- Lists for mutable integer-indexed Iterables, Tuples for immutable.
		- Dicts for key-value mappings.

2. Everything should be as fast as possible, targetting high performance computing.
	- Numba-accelerated and vectorized functions where it makes sense.
	- Memoization module for caching the results of expensive functions.
	






