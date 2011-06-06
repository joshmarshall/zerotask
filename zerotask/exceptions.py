""" The zerotask exceptions """

from zerotask import jsonrpc

class JSONRPCError(Exception):
    """ Holds data for the JSON-RPC error. """
    code = None
    message = None
    def __init__(self, code, message=None, *args, **kwargs):
        """ Sets the code and message """
        message = message or jsonrpc.MESSAGES.get(code, "Unknown code.")
        Exception.__init__(self, message, *args, **kwargs)
        self.code = code
        self.message = message

    def error_response(self, request_id=None):
        """ Returns a JSON-RPC dictionary with the passed request_id """
        return jsonrpc.error(self.code, request_id, self.message)
