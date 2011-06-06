""" The task decorator(s?) """

import types
from zerotask.dispatcher import Dispatcher


def task(name=None, dispatcher=None):
    """ A decorator for adding tasks to the dispatcher """
    func = None
    if type(name) == types.FunctionType:
        func = name
        name = func.__name__
    if not dispatcher:
        dispatcher = Dispatcher.instance()

    def wrap(fn):
        """ Passes the function to wrapper """
        dispatcher.add_handler(fn, name)
        return fn

    if func:
        return wrap(func)
    return wrap

