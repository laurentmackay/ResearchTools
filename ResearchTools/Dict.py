import hashlib
from itertools import product
from collections.abc import Iterable
import json
from typing import Any, Dict

def dict_product(d):
    '''
    Cartesian product for dicts, basically `itertools.product` but extended to work on a dict.
    
    Returns a list of dicts where the values are individual items of all `Iterable` values in the original dict `d`.
    '''
    keys = d.keys()
    prod = product(*[v if isinstance(v, Iterable) else (v, ) for v in d.values()])
    return [{k: v for k, v, in zip(keys, p)} for p in list(prod)]

def take_dicts(dict_list, filter):
    '''
    Takes any dicts from a list, `dict_list`, that matches all the key-value pairs in the `filter` dict.

    Return a list of dicts. 
    '''
    keys = filter.keys()
    N_keys = len(keys)
    return [d for d in dict_list if len([None for k in keys if k in d.keys()])==N_keys and len([None for k in keys if d[k]==filter[k]])==N_keys]


def dict_hash(dictionary: Dict[str, Any], pre_hash=None) -> str:
    """MD5 hash of a dictionary."""
    dhash = hashlib.sha384()
    # We need to sort arguments so {'a': 1, 'b': 2} is
    # the same as {'b': 2, 'a': 1}

    if pre_hash:
        dhash.update(pre_hash.encode('utf-8'))

    if not isinstance(dictionary, dict):
        for d in dictionary:
            encoded = json.dumps(d, sort_keys=True).encode()
            dhash.update(encoded)
    else:
            encoded = json.dumps(dictionary, sort_keys=True).encode()
            dhash.update(encoded)


    
    return dhash.hexdigest()

def last_dict_key(d):
    return next(reversed(d.keys()))

def last_dict_value(d):
    return d[last_dict_key(d)]

def first_dict_value(d):
    return d[first_item(d)]
