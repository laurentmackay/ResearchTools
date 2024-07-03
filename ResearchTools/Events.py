'''
Provides a `TimeBasedEventExecutor` that can be iterativley updated with the current 'time'.
If any events should have taken place between the last given time and the current time, they will be trigegred.
'''

def TimeBasedEventExecutor(events):
    '''Execute events after specific "times" have passed.

    Args:
        Events: List of 2- or 3- tuples. First item in each tuple is the time for each event, second item is the function to be executed for each event,
             third item (optional) is a string to be printed after the event function is executed.

    Returns:
        wait_and_excute: Function that can be called with signature wait_and_execute(t, *args), where `t` is the current "time" and `*args` are any argument
            to be passed to the event function. This function also has `extend` and `append` methods that allow for the event list to be grown using the same
            syntax as lists.

    ToDo:
        Add method to specify the time until the next event.

    '''
    def wait_and_execute(t, *args):
        fired=[]
        for evt in events: #iterate over events list and execute any fired events
            if t >= evt[0]:
                fired.append(evt)
                evt[1](*args)
                if len(evt) > 2:
                    print(evt[2])

        for evt in fired: #remove expired events from event list
            events.remove(evt)

        return len(fired)>0

    def sort():
        nonlocal events
        events=list(sorted(events, key=lambda x:x[0]))

    def append(x):
        events.append(x)
        sort()

    def extend(x):
        events.extend(x)
        sort()
        
    out = wait_and_execute
    out.append = append
    out.extend = extend
    out.events = events

    return out


def CreatePeriodicEvent(func, period, Executor, t=0):
    '''
    Creates a periodic event for the exection of `func` with period `period` to an existing TimeBasedEventExecutor `Executor`.
    The first time this event is fired is at time `t` (0 by default)
    '''

    from Caching import get_caller_locals

    def exec_and_queue(*args):
        t_prev=get_caller_locals()['evt'][0]
        func(*args)
        Executor.append((t_prev+period, exec_and_queue))
        

    Executor.append((t, exec_and_queue))

def EventListenerPair():

    fired=False

    def event(*_):
        nonlocal fired
        fired=True

    def listener(*_):
        nonlocal fired
        return fired

    return event, listener