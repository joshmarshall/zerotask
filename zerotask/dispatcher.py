""" The JSON RPC dispatcher """

from zerotask.exceptions import JSONRPCError
from zerotask import jsonrpc
import logging

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
        method = data.get('method')
        params = data.get("params", [])
        request_id = data.get("id")
        if method is None or type(params) not in (tuple, list, dict):
            logging.warning("Invalid request")
            if not request_id:
                return None
            return jsonrpc.error(jsonrpc.INVALID_REQUEST, request_id)
        if not self.handlers.has_key(method):
            logging.warning("Method %s not found.", method)
            if not request_id:
                return None
            return jsonrpc.error(jsonrpc.METHOD_NOT_FOUND, request_id)
        func = self.handlers.get(method)
        try:
            if type(params) in (tuple, list):
                result = func(*params)
            else:
                result = func(**params)
            result_obj = jsonrpc.result(result, request_id)
        except JSONRPCError, jerr:
            result_obj = jerr.error_response(request_id)
        except Exception, exc:
            error_message = str(exc)
            error_code = jsonrpc.INTERNAL_ERROR
            result_obj = jsonrpc.error(error_code, request_id, error_message)
        if not request_id:
            # Notification
            if result_obj.has_key("error"):
                logging.warning("Error: %s" % result_obj)
            return None
        return result_obj

    def add_handler(self, method, name=None):
        """ Adds a handler for dispatching """
        if not name:
            name = method.__name__
        logging.info("Adding method '%s'", name)
        self.handlers[name] = method

    def has_handler(self, name):
        """ Checks if the handler is available """
        if self.handlers.has_key(name):
            return True
        return False
