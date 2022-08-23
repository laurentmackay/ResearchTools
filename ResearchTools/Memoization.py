import inspect, os, pickle, sys

from .Filesystem import filename_without_extension
from .Dict import dict_hash

def get_caller():

    stack = inspect.stack(context=0)
    caller = stack[2]

    return caller.frame.f_globals[caller.function]


def get_caller_locals():

    stack = inspect.stack(context=0)
    caller = stack[2]

    return caller.frame.f_locals

def code_filename():
    func =  get_caller()
    return filename_without_extension(func.__code__.co_filename)


def signature_string(f=None, locals=None):
    '''Returns a human-readable string describing the signature of a function call. All passed positional arguments are appear in the string, while only non-default keyword arguments are included.

    Kwargs:
        f : callable object, or `None`. If `None`, the calling function is used.
        locals : dictionary of local values for the function call, or `None`. If `None`, the locals of the calling function is used.
    '''
    if f is None:
        f = get_caller()

    if locals is None:
        locals = get_caller_locals()

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


def function_savepath(func=None):
    if func is None:
        func = get_caller()

    filename = filename_without_extension(func.__code__.co_filename)
    funcname = func.__name__
    return os.path.join(filename, funcname)

def function_call_savepath(func=None, locals=None):
    if func is None:
        func = get_caller()

    if locals is None:
        locals = get_caller_locals()

    basepath = function_savepath(func)

    sig = signature_string(f=func, locals=locals)

    return os.path.join(basepath, sig)

def check_function_cache(func, load=True, kw={}, pre_hash=''):
    cache = os.path.join('.cache', function_savepath(func))
    if kw or pre_hash:
        cache = os.path.join(cache, dict_hash(kw, pre_hash=pre_hash))
    cache_dir = os.path.dirname(cache)
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)

    if os.path.exists(cache) and load:
        with open(cache, 'rb') as file:
            results = pickle.load(file)

    else:
        results = None

    return results, cache



def script_name():
    return filename_without_extension(sys.argv[0])
