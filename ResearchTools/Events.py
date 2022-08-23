def EventExecutor(events):

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