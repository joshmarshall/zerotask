""" The ZeroTask Client """

import zmq
import logging
import optparse
import re

class Client(object):
    """ A ZeroTask client instance """

    def __init__(self, request_uri, subscribe_uri=None):
        self._request_uri = request_uri
        if not subscribe_uri:
            # setting sub port to 5556 by default
            subscribe_uri = re.sub(":\d+", ":5556", request_uri)
        self._subscribe_uri = subscribe_uri

    def _dispatch(self, method, *args, **kwargs):
        """ Turns a request into a JSON-RPC call and calls it """
        req_obj = {"jsonrpc": "2.0", "method": "broker.new_task" }
        params = {"method": method }
        if args:
            params["params"] = args
        else:
            params["params"] = kwargs
        req_obj["params"] = params
        req_obj["id"] = None
        logging.info("Sending message %s", req_obj)
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(self._request_uri)
        socket.send_json(req_obj)
        result = socket.recv_json()
        logging.info("Recieved response %s", result)
        task_id = result["result"]["id"]
        ssocket = context.socket(zmq.SUB)
        ssocket.connect(self._subscribe_uri)
        ssocket.setsockopt(zmq.SUBSCRIBE, "")
        result_value = None
        while True:
            result = ssocket.recv_json()
            logging.info("Recieved response %s", result)
            if not result["method"] == "task.finished" or \
                result["params"]["id"] != task_id:
                logging.info("Not what we're waiting for...")
                continue
            req = {"method": "broker.task_result", "params": {"id": task_id}}
            socket.send_json(req)
            result = socket.recv_json()
            result_value = result["result"]
            break
        return result_value

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
    print "Result for 5+6:", client.add(5, 6)
    print "Result for 5-6:", client.subtract(5, 6)

if __name__ == "__main__":
    main()
