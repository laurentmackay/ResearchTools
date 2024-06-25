##########
#
# funcs.py
#
#
# Author: Clinton H. Durney
# Email: cdurney@math.ubc.ca
#
# Last Edit: 11/8/19
##########


from numba import jit
import math

from scipy.spatial import ConvexHull
from scipy.spatial import Delaunay
import numpy as np



@jit(nopython=True, cache=True, inline='always')
def euclidean_distance(A, B):
    #Euclidean distance from point A to B

    # dist = [(a - b)**2 for a, b in zip(A, B)]
    # dist = math.sqrt(sum(dist))
    d=B-A
    return np.sqrt(np.sum(d*d))

@jit(nopython=True, cache=True, inline='always')
def unit_vector(A,B):
    # Calculate the unit vector from A to B

    dist = euclidean_distance(A,B)

    return (B-A)/dist


@jit(nopython=True, cache=True, inline='always')
def unit_vector_and_dist(A, B):
    # Calculate the unit vector from A to B, and return the distance as well

    dist = euclidean_distance(A, B)

    return (B-A)/dist, dist

def unit_vector_2D(A,B):
    return unit_vector(A,B)[:2]



def convex_hull_volume(pts):
    return ConvexHull(pts).volume


@jit(nopython=True, cache=True, inline='always')
def cross33(a,b):
    # cross product between two 3-vectors
    return np.array([a[1]*b[2]-a[2]*b[1], a[2]*b[0]-a[0]*b[2],a[0]*b[1]-a[1]*b[0]])

@jit(nopython=True, cache=True, inline='always')
def cross3Mat(a,b):
    #cross product between a vector and an Nx3 matrix
    out = np.zeros((b.shape))
    for i in range(0,b.shape[0]):
        out[i,0]=a[1]*b[i,2]-a[2]*b[i,1]
        out[i,1]=a[2]*b[i,0]-a[0]*b[i,2]
        out[i,2]=a[0]*b[i,1]-a[1]*b[i,0]

    return out

@jit(nopython=True, cache=True, inline='always')
def crossMatMat(a,b):
    # pair-wise cross products of two Nx3 matrices
    out = np.zeros((b.shape))
    for i in range(0,b.shape[0]):
        out[i,0]=a[i,1]*b[i,2]-a[i,2]*b[i,1]
        out[i,1]=a[i,2]*b[i,0]-a[i,0]*b[i,2]
        out[i,2]=a[i,0]*b[i,1]-a[i,1]*b[i,0]

    return out    



def sort_corners(corners, center_pos, pos_nodes):

    corn_sort = [(corners[0],0)]
    u = unit_vector_2D(center_pos, pos_nodes[corners[0]])

    for i in range(1,len(corners)):
        v = unit_vector_2D(center_pos, pos_nodes[corners[i]])
        dot = np.dot(u,v)
        det = np.linalg.det([u,v])
        angle = np.arctan2(det,dot)
        corn_sort.append((corners[i],angle))
        corn_sort = sorted(corn_sort, key=lambda x: x[1])
        corn2 = [pos_nodes[entry[0]] for entry in corn_sort]
    
    return corn2, corn_sort

@jit(nopython=True, cache=True, inline='always')
def triangle_area_and_vector(pos_side):
    
    A_alpha = triangle_area_vector(pos_side)
    return np.linalg.norm(A_alpha), A_alpha
    

@jit(nopython=True, cache=True, inline='always')
def triangle_area_vector(pos_side):

    inds=np.array([2,0,1])
    A_alpha = np.sum(crossMatMat(pos_side,pos_side[inds]),axis=0)/2
    return A_alpha

@jit(nopython=True, cache=True, inline='always')
def triangle_areas_and_vectors(pos_side):

    A_alpha = triangle_area_vectors(pos_side)
    
    return np.array([np.linalg.norm(v) for v in A_alpha]), A_alpha

@jit(nopython=True, cache=True, inline='always')
def triangle_area_vectors(pos_side) -> np.ndarray:
    
    inds=np.array([2,0,1])
    A_alpha = 0.5*np.sum(np.cross(pos_side,pos_side[:,inds,:]), axis=1)
    
    return A_alpha







