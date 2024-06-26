import os, pickle, multiprocessing, psutil
from collections.abc import Iterable
from itertools import product
import signal

from pathos.pools import ProcessPool as Pool
from pathos.pools import ThreadPool as ThreadPool

import numpy as np

from .Iterable import args_nd_shape, first_item
from .Dict import hash384, dict_product, dict_product_nd_shape
from .Caching import cache_file, function_savedir,  signature_lists, signature_string

def sweep(*args, kw={}, expand_kw=True, savepath_prefix='.', extension='.pickle', overwrite=False, 
            pool=None, pre_process=None, pre_process_kw={}, pass_kw=False, dry_run=False, print_code=False,
            inpaint=None, cache=False, refresh=False, verbose=True, dtype=None):
    """
    Perform a sweep of a function over all parameter and keyword combinations, or retrieve corresponding results from local storage.

    Args:
        *args (function or Iterable): The function(s) and the Iterable(s) containing parameters that the function(s) are evaluated with. 
            If multiple parameter iterables are given, they will be expanded into a single Iterable using itertools.product. Any results
            that are not saved to storage by the function will be automatically saved.
            
        kw (dict or Iterable): Keyword dict(s) to be fed into the function(s). If a dict is given, all keys must be strings. 
            If an Iterable is given, all its members must be keyword dicts.

        inpaint (Any): If not None, do not evaluate any function(s), but replace any missing results with `inpaint`. 
            Default is None.

        savepath_prefix (str): The base path where results are to be saved/loaded. Default is the present working directory '.'.  

        extension (str): The file extension for the serialized results. Default is '.pickle'.

        overwrite (bool): Controls whether existing results are to be overwritten. Default is False.

        pool (Mappable worker pool): The worker pool that carries out the function evaluations. If None (default), 
            then a mulitprocessing pool with n-1 nodes is created, where n is the number of physical cores on the machine.

        pre_process (function): A function for pre-processing results after function execution or loading from storage.
            Default is None.

        cache (bool): If pre_process is not None, the processed results are stored in a local cache for rapid later retrieval.
             Default is False.

        refresh (bool): If pre_process is not None and cache is True, ignore the current cache and overwrite it after processing. 
            Default is False.
        
        verbose (bool): Controls whether a message is printed when files are loaded

    """
    def get_callables(*args):
        """Get all the functions that were passed"""
        return [a for a in args if callable(a)]

    def get_iterables(*args):
        """Get all the Iterables that were passed"""
        return [a for a in args if isinstance(a, Iterable)]

    func = get_callables(*args)
    par0 = get_iterables(*args)
    params = tuple(product(*par0))

    if len(func) > 1:
        return [sweep(f, params, savepath_prefix=savepath_prefix, extension=extension, overwrite=overwrite, pool=pool, pre_process=pre_process, inpaint=inpaint, cache=cache) for f in func]
    elif len(func) < 1:
        raise TypeError('No function specified')
    else:
        func = func[0]

    extension = extension.strip()
    if extension[0] != '.':
        extension = '.'+extension
    kw0=kw
    if isinstance(kw, dict):
        if expand_kw:
            kw=dict_product(kw)
        else:
            kw = [kw]

    savepath = function_savedir(func)
    basedir = os.path.join(savepath_prefix, savepath)
    results = None
    shape = (max(len(params), 1),  len(kw))

    if cache is not None and pre_process is not None:
        if cache == True:
            filename  = hash384(pre_process_kw, pre_hash=hash384([*params, *kw, savepath]))

            cache  = cache_file(pre_process, filename=filename, cache_dir='.cache')

            if (not (refresh or overwrite)) and os.path.exists(cache):
                    with open(cache, 'rb') as file:
                        results = pickle.load(file)



    if results is None or not (results.shape == shape):
        results = np.array([[None]*shape[1]]*shape[0])

    if pool is None:
        core_count = psutil.cpu_count(logical=False)
        pool = Pool(nodes=core_count-1)
        if print_code:
            code_string = f"from pathos.multiprocessing import ProcessPool\npool = ProcessPool(nodes={core_count-1})\n"

      
    #setup a graceful exit
    signal.signal(signal.SIGTERM, lambda a,b: pool.terminate())

    results_raveled = results.ravel()

    par_names, _ = signature_lists(func)

    savepaths = multiprocessing.Manager().dict()
    func_name = func.__name__

    if print_code:
        pathos_args_sequence = multiprocessing.Manager().list()
    
    def check_filesystem_and_run(k):

        i, j = np.unravel_index(k, shape)

        pars = params[i] if params else params

        if not isinstance(pars, Iterable):
            pars = (pars,)
        locals = {**{a: v for a, v in zip(par_names, pars)}, **kw[j]}

        path = os.path.join(basedir, signature_string( f=func, locals=locals) + extension)

        savepaths[k] = path
        missing_file = not os.path.exists(path)
        run = inpaint is None and (overwrite or missing_file)

        if run:
            if verbose:
                kw_str=", ".join([ str(k) + "=" + str(v) for k,v in kw[j].items()])
                pars_str = ", ".join([str(p)  for p in pars])
                print(f"running: {func_name}({pars_str}, {kw_str})")
                # print(f"pool.apipe({func_name},{pars_str}, {kw_str})")

            if print_code:
                pathos_args_sequence.append(f'({pars},{kw[j]}),')

            if not dry_run:
                result = func(*pars, **kw[j])
            else:
                result=None

            #save the result for later, if that makes sense
            if result is not None and not os.path.exists(path):
                if  not os.path.exists(basedir):
                    os.makedirs(basedir)

                with open(path, 'wb') as file:
                    pickle.dump(result, file)
        else:
            result = None

        return result



    to_check = np.where(results_raveled == None)[0]

    possibly_new_results = len(to_check) > 0

    # signal.pthread_sigmask(signal.SIG_BLOCK,[signal.SIGINT])


    results_raveled[to_check] = pool.map(check_filesystem_and_run, to_check)

    if print_code:
        code_string+=f"pool.map(lambda args_and_kws: {func_name}(*args_and_kws[0], **args_and_kws[1]),(\n"+"\n".join(pathos_args_sequence)+"\n))"
        print(code_string)


    # signal.pthread_sigmask(signal.SIG_UNBLOCK,[signal.SIGINT])

    inpaint_ij = multiprocessing.Manager().list()
    kw_pre=pre_process_kw
    
    if pass_kw:
        pre_process_kw0 = pre_process_kw

    def loader(k):
        nonlocal kw_pre # this nonlocal declaration shouldn't be necessary, but fixes a bug in some pyhton 3.10 versions

        i,j = np.unravel_index(k, shape)
        if results[i,j] is None:
            path = savepaths[k]
            file_exists = os.path.exists(path)
            if file_exists:  
                size = os.path.getsize(path)              
                with open(path, 'rb') as file:
                    try:
                        if not dry_run:
                            if pre_process is None:
                                out = pickle.load(file)
                            else:
                                if pass_kw:
                                    kw_pre={**pre_process_kw0, **kw[j]}

                                out =  pre_process(pickle.load(file), **kw_pre)
                        else:
                            out=None

                        if verbose:
                            print(f'loaded[{i}][{j}]: {path}  ({size/(1024*1024)} mb)')

                        result =  out
                    except Exception as e:
                        
                        if verbose:
                            #print(f'exception loading/processing: {path}  ({size/(1024*1024)} mb)')
                            print(f'rm {path}  ')
                        result = None
                        raise e

            else:
                # print(f'inpainting[{i}][{j}]:')
                result = None

            if result is None:
                inpaint_ij.append(k)


            return result


  
      

    if pre_process is None:
        pool = ThreadPool(nodes=psutil.cpu_count()-1) #revert to simple if we just have threading for IO-bound stuff
        
    needed = np.where(results_raveled==None)[0] 

    results_raveled[needed] = pool.map(loader, needed) #multiprocessing for CPU-bound stuff


    # results = np.reshape( results, shape)

    if possibly_new_results and pre_process is not None and cache:
        with open(cache, 'wb') as file:
                pickle.dump(results, file)
    
    #inpaint after we have cached
    if inpaint is not None:
        for k in inpaint_ij:
            results_raveled[k] = inpaint

    if dtype is None:
        types=set([type(r) for r in results_raveled])
        is_scalar = [(not hasattr(r,'shape') ) or r.size==1 for r in results_raveled]
        if np.all(is_scalar) and equivalent_classes(types):
            dtype = first_item(types)
    
    try:
        if dtype is not None:
            results=np.array(results, dtype = dtype )

        if isinstance(kw0, dict) and expand_kw:
            results = results.reshape(*[*args_nd_shape(*par0),*dict_product_nd_shape(kw0)])
    except:
        pass
    
    return results


def equivalent_classes(types):
    types=tuple(types)
    return np.all([issubclass(t,types[0]) for t in types])