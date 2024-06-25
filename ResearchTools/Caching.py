import inspect, os, pickle, sys

from .Filesystem import filename_without_extension
from .Dict import hash384

def get_caller():

    stack = inspect.stack(context=0)
    caller = stack[2]

    return caller.frame.f_globals[caller.function]


def get_caller_locals():

    stack = inspect.stack(context=0)
    caller = stack[2]

    return caller.frame.f_locals

def code_filename():
    f =  get_caller().__code__.co_filename
    return filename_without_extension(f)

def keyword_vals(**kw):

    locals = {**get_caller_locals(), **kw}


    _, kws = signature_lists(get_caller())

    return {p:locals[p] for p in kws if p in locals.keys()}
        
    

def signature_string(f=None, locals=None, **kw):
    '''Returns a human-readable string describing the signature of a function call. All passed positional arguments are appear in the string, while only non-default keyword arguments are included.

    Kwargs:
        f : callable object, or `None`. If `None`, the calling function is used.
        locals : dictionary of local values for the function call, or `None`. If `None`, the locals of the calling function is used.
        **kw : any other passed keywords will override those found in locals
    '''
    if f is None:
        f = get_caller()

    if locals is None:
        locals = get_caller_locals()

    locals = {**locals, **kw}

    sig = inspect.signature(f)

    args, kws = signature_lists(f)

    arg_strs = [str(locals[p]) for p in args]

    kw_strs = []

    for p in kws:
        if p in locals.keys():
            val = locals[p]
            default = sig.parameters[p].default
            if default != val:
                if not isinstance(default, bool):
                    kw_strs.append(p+'='+str(locals[p]))
                elif default == True:
                    kw_strs.append('no_'+p)
                else:
                    kw_strs.append(p)

    out = ''

    if len(kw_strs):
        out += '_'.join(kw_strs)

    if len(arg_strs):
        new = ','.join(arg_strs)
        if len(out):
            out += '_'+new
        else:
            out = new
    elif len(out) == 0:
        out = '_'

    return out


def signature_lists(f):
    '''Returns ths argument and keyword names of the function `f`.'''
    sig = inspect.signature(f)

    args = []
    kws = []

    for p in sig.parameters:
        if sig.parameters[p].default is not inspect.Parameter.empty:
            kws.append(p)
        else:
            args.append(p)

    return args, kws


def function_savedir(func):

    filename = filename_without_extension(func.__code__.co_filename)
    funcname = func.__name__
    return os.path.join(filename, funcname)


def cache_file(func=None, locals=None, cache_dir='.', filename=None, **kw):
    if func is None:
        func = get_caller()

    funcpath = function_savedir(func)

    if filename is None:
        if locals is None:
            locals = get_caller_locals()

        sig = signature_string(f=func, locals=locals, **kw)

        return os.path.join(cache_dir, funcpath, sig + '.pickle')
    else:
        return os.path.join(cache_dir, funcpath, filename)



def cached(func=None, locals=None,   cache_dir='.', **kw):
    ''' returns the cached result of the current function, with any keyword replacements in **kw'''




    if func is None:
        func = get_caller()

    if locals is None:
        locals = get_caller_locals()

    path = cache_file(func=func, locals=locals, cache_dir=cache_dir, **kw)

    if os.path.exists(path):
            with open(path, 'rb') as file:
                return pickle.load(file)
    else:
       return None

def cached_call(_ , *arg,   cache_dir='.', **kw):
    ''' returns the cached result of a function call, if no cached result exists run the function and cache the result'''

    #need to somehow get the positional arg names of the function and map *arg onto those, probably can use the locals interface....
    path = cache_file(func=_, locals={}, cache_dir=cache_dir, **kw)

    if os.path.exists(path):
            with open(path, 'rb') as file:
                return pickle.load(file)
    else:
       return None

# def check_function_cache(func, load=True, kw={}, pre_hash=''):
#     cache = os.path.join('.cache', function_savedir(func))
#     result = cached(func, load=load, filename=hash384(kw, pre_hash=pre_hash), cache_dir='.cache')
#     if kw or pre_hash:
#         cache = os.path.join(cache, )
#     cache_dir = os.path.dirname(cache)
#     if not os.path.exists(cache_dir):
#         os.makedirs(cache_dir)

#     if os.path.exists(cache) and load:
#         with open(cache, 'rb') as file:
#             results = pickle.load(file)

#     else:
#         results = None

#     return results, cache



def script_name():
    return filename_without_extension(sys.argv[0])
