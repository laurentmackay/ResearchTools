def RK_handler(f, order=2):
    """
    Implements 2,3,4-order Ralston RK methods

    Returns a function that can be called to get the RK-increment.
    """
    ############### CONSTANTS ###############
    two_thirds=2/3
    one_quarter=1/4
    three_quarters=3/4
    one_sixth=1/4
    ############# END CONSTANTS #############

    if order==2:


        def RK_delta(h, f0, x0, *args):

            k1 = h*f0
            k2 = h*f(x0 + two_thirds*k1, *args)
    
            return one_quarter*k1 + three_quarters*k2

    elif order==3:
        
        def RK_delta(h, f0, x0, *args):

            k1 = h*f0
            k2 = h*f(x0 + 0.5*k1, *args)
            k3 = h*f(x0 - k1 + 2*k2, *args)
        
            return one_sixth*(k1+4*k2+k3)

    elif order==4:

        def RK_delta(h, f0, x0, *args):

            k1 = h*f0
            k2 = h*f(x0 + .4*k1, *args)
            k3 = h*f(x0 + .29697761*k1 + .15875964*k2,*args)
            k4 = h*f(x0 + .21810040*k1  - 3.05096516*k2  + 3.83286476*k3, *args)

            return .17476028*k1  - .55148066*k2 + 1.20553560*k3 + .17118478*k4

    return RK_delta