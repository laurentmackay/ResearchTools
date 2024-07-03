'''Module that extends the functionality of python iterables.'''

from collections.abc import Iterable

def first_item(iter):
    '''return the first item of an iterable, works even when you cannot use integer subscripting'''
    return iter.__iter__().__next__()

def args_nd_shape(*args):
    '''ND-array shape of the result of `itertools.product(*args)` as a tuple of ints.'''
    return tuple(len(v)  for v in args if isinstance(v, Iterable))

def imin(iter):
    '''returns the location of the `min` of an iterable'''
    return min(range(len(iter)), key=iter.__getitem__)

def min_and_loc(iter):
    '''returns the `min` of an iterable and its location'''
    i=imin(iter)
    return iter(i), i

def imax(iter):
    '''returns the location of the `max` of an iterable'''
    return max(range(len(iter)), key=iter.__getitem__)

def max_and_loc(iter):
    '''returns the `max` of an iterable and its location'''
    i=imax(iter)
    return iter(i), i