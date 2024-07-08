'''Various utility functions, mainly used to bring the idiosyncratic numpy syntax to something closer to MATLAB.'''

import numpy as np

def find(x):
    return np.argwhere(x).flatten()

def find_first(x):
    return np.argwhere(x)[0,0]