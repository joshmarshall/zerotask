""" The task decorator(s?) """

import types
from zerotask.dispatcher import Dispatcher

class task(object):
    """ A decorator class for adding tasks to the dispatcher """

    def __new__(cls, name=None, dispatcher=None):
        """ Checks for name is a function or not """
        func = None
        if type(name) == types.FunctionType:
            # Decorator called without options -- set defaults
            func = name
            name = func.__name__
            dispatcher = Dispatcher.instance()
        instance = object.__new__(cls)
        instance.name = name
        instance.dispatcher = dispatcher
        if func:
            # Return 'wrapped' function
            return instance(func)
        else:
            # Decorator called with options -- just instantiate
            return instance


    def __init__(self, name=None, dispatcher=None):
        self.name = name
        self.dispatcher = dispatcher or Dispatcher.instance()

    def __call__(self, func):
        """ Add function to dispatcher and return function """
        self.dispatcher.add_handler(func, self.name)
        return func
