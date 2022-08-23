import sys
import multiprocessing

IS_WINDOWS = sys.platform.startswith('win')

if IS_WINDOWS:
    import dill

    def run_dill(payload, args):
        #load a function from its dilled state and run it with *args.
        fun = dill.loads(payload)
        return fun(*args)

    def make_dill(f, args):
        #create a dilled function payload and argument pair. The function can then be executed using `run_dill` on the returned tuple.
        return (dill.dumps(f), args)


def Process(target, args=tuple(), name=None, kwargs={}, daemon=None):
    '''Convenience function that increases the robustness of creating processes on the Windows platform, equivalent to `multiprocessing.Process` on Unix platforms.
    
    Args:
        target : Callable object to be invoked by `run()`
    Kwargs:
        args :  argument tuple for the target invocation.
        kwargs : dictionary of keyword arguments for the target invocation.
        daemon : sets the process daemon flag to `True` or `False`. If `None` (the default), this flag will be inherited from the creating process.
    Returns:
        a `multiprocessing.Process` object
     '''
    if IS_WINDOWS:
        proc = multiprocessing.Process(target=run_dill, name=name, args=make_dill(target, args), daemon=daemon, kwargs=kwargs)
    else:
        proc = multiprocessing.Process(target=target, name=name, args=args, kwargs=kwargs, daemon=daemon)
    return proc
