""" The zerotask exceptions """

class JSONRPCError(Exception):
    """ Holds data for the JSON-RPC error. """
    code = None
    message = None
    def __init__(self, code, message, *args, **kwargs):
        """ Sets the code and message """
        Exception.__init__(self, message, *args, **kwargs)
        self.code = code
        self.message = message
