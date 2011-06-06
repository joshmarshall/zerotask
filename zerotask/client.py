""" The ZeroTask Client """

import zmq
import logging
import optparse
import re
from zerotask import jsonrpc
from zerotask.exceptions import JSONRPCError

class Client(object):
    """ A ZeroTask client instance """

    def __init__(self, request_uri, subscribe_uri=None):
        self._request_uri = request_uri
        self._client_id = None
        if not subscribe_uri:
            # setting sub port to 5556 by default
            subscribe_uri = re.sub(":\d+", ":5556", request_uri)
        self._subscribe_uri = subscribe_uri
        self.context = zmq.Context()
        self._req_socket = self.context.socket(zmq.REQ)
        self._req_socket.connect(self._request_uri)
        self._sub_socket = self.context.socket(zmq.SUB)
        self._sub_socket.connect(self._subscribe_uri)
        self._sub_socket.setsockopt(zmq.SUBSCRIBE, "")
        self._client_id = self._get_client_id()

    def _get_client_id(self):
        """ Tries to connect to broker and get client id """
        method = "zerotask.broker.client_connect"
        params = []
        request = jsonrpc.request(method, params)
        self._req_socket.send_json(request)
        response = self._req_socket.recv_json()
        if response.has_key("error"):
            raise JSONRPCError(response["error"]["code"],
                               response["error"].get("message"))
        client_id = response.get("result")
        if not client_id:
            raise JSONRPCError(jsonrpc.INVALID_CLIENT_ID)
        logging.info("New client id: %s", client_id)
        return client_id

    def _dispatch(self, method, *args, **kwargs):
        """ Turns a request into a JSON-RPC call and calls it """
        req_method = "zerotask.broker.client_new_task"
        req_params = dict(client_id=self._client_id, method=method)
        if args:
            req_params["params"] = args
        else:
            req_params["params"] = kwargs
        req_obj = jsonrpc.request(req_method, req_params)
        logging.info("Sending message %s", req_obj)
        self._req_socket.send_json(req_obj)
        result = self._req_socket.recv_json()
        logging.info("Recieved response %s", result)
        if result.get("error"):
            raise JSONRPCError(result["error"]["code"],
                               result["error"].get("message"))
        task_id = result["result"]
        logging.info("New task id: %s", task_id)
        result_response = None
        while True:
            sub_result = self._sub_socket.recv_json()
            logging.info("Recieved response %s", sub_result)
            sub_method = sub_result.get("method")
            sub_params = sub_result.get("params")
            if sub_method != "zerotask.client.task_result_ready":
                logging.info("Method %s not important.", sub_method)
                continue
            if sub_params.get("task_id") != task_id:
                logging.info("%s != %s" % (sub_params.get("task_id"), task_id))
                continue
            result_method = "zerotask.broker.client_task_result"
            result_params = dict(client_id=self._client_id,
                                 task_id=task_id)
            result_req = jsonrpc.request(result_method, result_params)
            self._req_socket.send_json(result_req)
            result_response = self._req_socket.recv_json()
            break
        if result_response.has_key("error"):
            raise JSONRPCError(result_response["error"]["code"],
                               result_response["error"].get("message"))
        return result_response.get("result")

    def __getattr__(self, attr):
        """ Returns an attribute tree """
        return AttribTree(self, attr)

class AttribTree(object):
    """ Just a holder for the namespace of a method """

    def __init__(self, client, base):
        self.client = client
        self.namespace = base

    def __getattr__(self, attr):
        """ Updates the namespace """
        self.namespace += ".%s" % attr
        return self

    def __call__(self, *args, **kwargs):
        """ Calls the client's dispatcher with updated namespace """
        return self.client._dispatch(self.namespace, *args, **kwargs)


def main():
    """ Tests a server's "add" call """
    options = optparse.OptionParser()
    options.add_option("-r", "--request_port", dest="req_port", type="int",
                       default=5555, help="the broker's request port")
    options.add_option("-s", "--subscribe_port", dest="sub_port", type="int",
                       default=5556, help="the broker's subscribe port")
    options.add_option("-a", "--address", dest="address",
                       default="*", help="the broker's address")
    options.add_option("-l", "--loglevel", dest="loglevel",
                       default="WARNING", help="INFO|WARNING|ERROR")

    opts, args = options.parse_args()
    logging.getLogger().setLevel(getattr(logging, opts.loglevel))
    logging.info("Starting client for request port %d", opts.req_port)
    logging.info("Starting client for subscribe port %d", opts.sub_port)
    client = Client(request_uri="tcp://%s:%s" % (opts.address, opts.req_port),
                    subscribe_uri="tcp://%s:%s" % (opts.address, opts.sub_port))
    for i in range(1000):
        print "Result for 5+6:", client.add(5, 6)
        print "Result for 5-6:", client.subtract(5, 6)

if __name__ == "__main__":
    main()
