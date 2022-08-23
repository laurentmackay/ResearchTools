def TimeBasedEventExecutor(events):
    '''Execute events after specific "times" have passed.

    Args:
        Events: List of 2- or 3- tuples. First item in each tuple is the time for each event, second item is the function to be executed for each event,
             third item (optional) is a string to be printed after the event function is executed.

    Returns:
        wait_and_excute: Function that can be called with signature wait_and_execute(t, *args), where `t` is the current "time" and `*args` are any argument
            to be passed to the event function. This function also have `extend` and `append` methods that allow for the event list to be grown.

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


    out = wait_and_execute
    out.append = events.append
    out.extend = events.extend
    out.events = events

    return out




def EventListenerPair():

    fired=False

    def event(*_):
        nonlocal fired
        fired=True

    def listener(*_):
        nonlocal fired
        return fired

    return event, listener