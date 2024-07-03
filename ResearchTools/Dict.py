'''
Module with a number of convenience functions for native Python dictionaries.
'''

import hashlib
from itertools import product
from collections.abc import Iterable
import json
import os
from typing import Any, Dict
import numpy as np
from .Iterable import first_item

def dict_product(d):
    '''
    Cartesian product for dicts, basically `itertools.product` but extended to work on a dict.
    
    Returns a list of dicts where the values are individual items of all `Iterable` values in the original dict `d`.
    '''
    keys = d.keys()
    prod = product(*[v if isinstance(v, Iterable) else (v, ) for v in d.values()])
    return [{k: v for k, v, in zip(keys, p)} for p in list(prod)]

def dict_product_nd_shape(d):
    '''
    ND-array shape of the result of `dict_product(d)`
    
    Returns a tuple of ints.
    '''
    return tuple(len(v)  for v in d.values() if isinstance(v, Iterable))

    
def take_dicts(dict_list, filter):
    '''
    Takes any dicts from a list, `dict_list`, that matches all the key-value pairs in the `filter` dict.

    Return a list of dicts. 
    '''
    mask = dict_mask(dict_list, filter)
    return [d for d, m in zip(dict_list, mask) if m]

def dict_mask(dict_list, filter):
    '''
    Return a bool mask of the items in dictionary list, `dict_list`, that match all the key-value pairs in the `filter` dictionary.
    '''
    keys = filter.keys()
    N_keys = len(keys)
    return [len([None for k in keys if k in d.keys()])==N_keys and len([None for k in keys if d[k]==filter[k]])==N_keys for d in dict_list  ]

def hash384(obj, pre_hash=None):
    '''
    sha384 hash of an object.
    
    This has been placed in the Dict module since dicts are the only builtin python types that cannot be directly hashed.

    ToDo: Consider if this is really appropriate location, maybe caching is better?
    '''
    dhash = hashlib.sha384()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}

    if pre_hash:
        dhash.update(pre_hash.encode('utf-8'))

    if not isinstance(obj, dict):
        for d in obj:
            encoded = json.dumps(d, sort_keys=True).encode()
            dhash.update(encoded)
    else:
            copy = obj.copy()
            join = os.path.join
            for k,v in copy.items():
                if callable(v):
                    if hasattr(v,'__code__'):
                        copy[k] = join(v.__code__.co_filename, v.__code__.co_name)
                    else:
                       copy[k] = v.__str__()

            encoded = json.dumps(copy, sort_keys=True).encode()
            dhash.update(encoded)


    
    return dhash.hexdigest()

def last_dict_key(d):
    '''returns the 'last' key in a dictionary key iterator, usually the most recently inserted key (not guranteed for all implementations) '''
    return next(reversed(d.keys()))

def last_dict_value(d):
    '''returns the last value in the dictionary. see `last_dict_key`'''
    return d[last_dict_key(d)]

def keys(d):
    '''returns a np.array of all the keys in the dictionary'''
    return np.array([ k for k in d.keys() ])

def closest_dict_value(d, key):
    ''' find closest match to the provided key, only suitable for dictionaries with numerical keys'''
    d_keys = keys(d)
    imin  = np.argmin(np.abs(d_keys-key))
    closest = d_keys[imin]
    return d[closest]

def first_dict_value(d):
    return d[first_item(d)]
