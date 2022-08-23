def first_item(iter):
    return iter.__iter__().__next__()


def imin(iter):
    return min(range(len(iter)), key=iter.__getitem__)

def min_and_loc(iter):
    i=imin(iter)
    return iter(i), i

def imax(iter):
    return max(range(len(iter)), key=iter.__getitem__)

def max_and_loc(iter):
    i=imax(iter)
    return iter(i), i