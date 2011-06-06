""" JSON-RPC helpers """

import random
import string

# Default messages
MESSAGES = { -32700: "Parse error.",
             -32600: "Invalid JSON-RPC request.",
             -32601: "Method not found.",
             -32602: "Invalid parameters.",
             -32603: "Internal error.",
             # Custom additions
             -32010: "Invalid client id.",
             -32020: "Invalid task id.",
             -32030: "Invalid node id.",
             -32050: "Task not complete." }

# Constants for readability
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMETERS = -32602
INTERNAL_ERROR = -32603
INVALID_CLIENT_ID = -32010
INVALID_TASK_ID = -32020
INVALID_NODE_ID = -32030
TASK_NOT_COMPLETE = -32050


def get_random_id():
    """ Generate a random id """
    return "".join([random.choice(string.letters) for i in range(20)])

def request(method, params=[], **kwargs):
    """ Generates a valid JSON-RPC 2.0 request.
    If id=None, the request is a notification.
    """
    request_id = kwargs.get("id", get_random_id())
    json_request = dict(jsonrpc="2.0",
                        method=method,
                        params=params)
    if request_id is not None:
        # Not a notification
        json_request["id"] = request_id
    return json_request

def result(result, request_id, **kwargs):
    """ Generates a valid JSON-RPC 2.0 result. """
    json_result = dict(jsonrpc="2.0",
                       result=result,
                       id=request_id)
    return json_result

def error(code, request_id, message=None, **kwargs):
    """ Generates a valid JSON-RPC 2.0 error.
    Uses message array if no message is passed.
    Adds any additional keyword arguments to error object.
    """
    message = message or MESSAGES.get(code, "Unknown error code.")
    json_error = dict(jsonrpc="2.0",
                      error=dict(code=code, message=message))
    if request_id:
            json_error["id"] = request_id
    json_error['error'].update(kwargs)
    return json_error
