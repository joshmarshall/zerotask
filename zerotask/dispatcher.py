""" The JSON RPC dispatcher """

from zerotask.exceptions import JSONRPCError

class Dispatcher(object):
    """ The dispatch service for JSON-RPC calls """

    _instance = None

    def __init__(self):
        self.handlers = {}
        self.context = None
        self.socket = None

    @classmethod
    def instance(cls):
        """ Returns the thread singleton """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def dispatch(self, data):
        """ Dispatches a JSON-RPC call """
        method = data['method']
        params = data.get("params", [])
        func = self.handlers.get(method, error(-32601, "Method unknown"))
        try:
            if type(params) in (tuple, list):
                result = func(*params)
            else:
                result = func(**params)
            result_obj = {"jsonrpc": "2.0", "result": result, "id": None}
        except JSONRPCError, jerr:
            error_result = {'code': jerr.code, 'message': jerr.message}
            result_obj = {"jsonrpc": "2.0", 'error': error_result, 'id': None}
        return result_obj

    def add_handler(self, method, name=None):
        """ Adds a handler for dispatching """
        if not name:
            name = method.__name__
        self.handlers[name] = method

    def has_handler(self, name):
        """ Checks if the handler is available """
        if self.handlers.has_key(name):
            return True
        return False


def error(errnum, message="JSON-RPC Error"):
    """ Returns a JSON-RPC error function """
    def jsonrpc_error(*args, **kwargs):
        """ Raises a JSON-RPC error """
        raise JSONRPCError(errnum, message)
    return jsonrpc_error
