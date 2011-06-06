""" The Node class """

import zmq
import logging
from zerotask.server import Server
from zerotask.task import task
from zerotask import jsonrpc
from zerotask.exceptions import JSONRPCError
import optparse

class Node(Server):
    """ A simple example server """

    namespace = "zerotask.node"

    def __init__(self, **kwargs):
        Server.__init__(self)
        self.broker_req_socket = None
        self.broker_sub_socket = None
        self.node_id = kwargs.get("node_id", None)

    def setup(self):
        """ Sets up the handlers """
        self.add_handler(self.task_ready)
        self.add_handler(self.request_status)

    def start(self):
        """ Checks if broker is setup, then starts the loop """
        if not self.broker_req_socket or not self.broker_sub_socket:
            self.add_broker("tcp://127.0.0.1:5555", "tcp://127.0.0.1:5556")
        Server.start(self)

    def add_broker(self, broker_req_uri, broker_sub_uri):
        """ Sets the (only) broker """
        self.broker_req_socket = self.context.socket(zmq.REQ)
        self.broker_sub_socket = self.context.socket(zmq.SUB)
        self.broker_req_socket.connect(broker_req_uri)
        self.broker_sub_socket.connect(broker_sub_uri)
        self.broker_sub_socket.setsockopt(zmq.SUBSCRIBE, "")
        # Getting node id from broker
        connect_method = "zerotask.broker.node_connect"
        connect_request = jsonrpc.request(connect_method)
        self.broker_req_socket.send_json(connect_request)
        connect_result = self.broker_req_socket.recv_json()
        if connect_result.has_key("error"):
            connect_error = connect_result["error"]
            raise JSONRPCError(connect_error["code"],
                               connect_error.get("message"))
        node_id = connect_result.get("result")
        if not node_id:
            raise JSONRPCError(jsonrpc.INVALID_NODE_ID)
        self.node_id = node_id
        logging.info("New node id: %s", node_id)
        self.add_callback(self.broker_sub_socket, self.subscribe)

    def subscribe(self, message):
        """ Special dispatching """
        logging.info("Received message %s", message)
        result = self.dispatcher.dispatch(message)
        if result:
            self.broker_req_socket.send_json(result)
            self.broker_req_socket.recv_json()

    def task_ready(self, method, task_id):
        """ Checks if node support the method, and responds if so. """
        if not self.dispatcher.has_handler(method):
            logging.info("Ignoring -- method %s is not supported.", method)
            return None # we don't support that method
        logging.info("Responding for new task %s", method)
        req_params = dict(node_id=self.node_id, task_id=task_id)
        req_method = "zerotask.broker.node_task_request"
        request = jsonrpc.request(req_method, req_params)
        self.broker_req_socket.send_json(request)
        result = self.broker_req_socket.recv_json()
        if result.has_key("error"):
            raise JSONRPCError(result["error"]["code"],
                               result["error"].get("message"))
        task_result = result.get("result")
        if not task_result:
            return None # we were not elected
        method = task_result['method']
        params = task_result.get("params", [])
        local_request = jsonrpc.request(method, params)
        # This is where we'd fire off to workers eventually...
        # This will be broken up here, and fire finished / failed
        # asynchronously based on workers PUSH / PULL results
        local_result = self.dispatcher.dispatch(local_request)
        if not local_result:
            logging.warning("Why do we have an empty result??")
        result_method = "zerotask.broker.node_task_finished"
        result_params = dict(node_id=self.node_id,
                             task_id=task_id)
        if local_result.has_key("error"):
            result_method = "zerotask.broker.node_task_failed"
            result_params["error"] = local_result["error"]
        else:
            result_params["result"] = local_result["result"]
        result_req = jsonrpc.request(method=result_method,
                                     params=result_params,
                                     id=None) # a notification
        self.broker_req_socket.send_json(result_req)
        self.broker_req_socket.recv_json()

    def request_status(self):
        """ Calls broker with the current node status """
        notify_method = "zerotask.broker.node_status"
        notify_params = dict(node_id=self.node_id,
                             workers=1)
        notification = jsonrpc.request(method=notify_method,
                                       params=notify_params,
                                       id=None) # notification
        logging.info("Sending status message: %s" % notification)
        self.broker_req_socket.send_json(notification)
        self.broker_req_socket.recv_json()
        return None


def main():
    """ Setup up basic node with add / subtract methods. """
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
    logging.info("Starting with broker request port %d", opts.req_port)
    logging.info("Starting with broker subscribe port %d", opts.sub_port)

    @task
    def add(first, second):
        """ just an addition method test """
        return first + second

    @task(name="subtract")
    def subtract_method(first, second):
        """ Testing task name attribute """
        return first - second

    node = Node()
    broker_req_uri = "tcp://%s:%s" % (opts.address, opts.req_port)
    broker_sub_uri = "tcp://%s:%s" % (opts.address, opts.sub_port)
    node.add_broker(broker_req_uri, broker_sub_uri)
    node.start()

if __name__ == "__main__":
    main()
