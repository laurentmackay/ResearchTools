import sys
import multiprocessing

IS_WINDOWS = sys.platform.startswith('win')

if IS_WINDOWS:
    import dill

    def run_dill(payload, args):
        fun = dill.loads(payload)
        return fun(*args)

    def make_dill(f, a):
        return (dill.dumps(f), (a,))


def mkprocess(target, args=tuple(), daemon=True):
    if IS_WINDOWS:
        proc = multiprocessing.Process(target=run_dill, args=make_dill(target, *args), daemon=daemon)
    else:
        proc = multiprocessing.Process(target=target, args=args, daemon=daemon)
    return proc
