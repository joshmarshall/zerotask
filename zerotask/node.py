""" The Node class """

import zmq
import logging
from zerotask.server import Server
from zerotask.task import task
from zerotask.dispatcher import Dispatcher
import optparse

class Node(Server):
    """ A simple example server """

    def __init__(self, **kwargs):
        Server.__init__(self)
        self.dispatcher = kwargs.get("dispatcher", Dispatcher.instance())
        self.broker_req_socket = None
        self.broker_sub_socket = None

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
        self.add_callback(self.broker_req_socket, self.dispatch)
        self.add_callback(self.broker_sub_socket, self.subscribe)

    def subscribe(self, message):
        """ Special dispatching """
        # This is bad stuff, needs to go into custom dispatcher, etc.
        if message['method'] == "node.task_ready":
            method = message["params"]['method'] # bad, bad, bad...
            task_id = message["params"]['id'] # so bad...
            if self.dispatcher.has_handler(method):
                self.broker_req_socket.send_json({"method": "node.task_request",
                                                  "params": {"id": task_id}})
        elif message["method"] == "node.request_status":
            self.broker_req_socket.send_json({"method": "node.status",
                                              "params": {"workers": 1}})

    def dispatch(self, message):
        """ Wrapper for dispatcher.dispatch """
        logging.info("Received message %s", message)
        result = message.get("result")
        if not message["result"]:
            # We were not elected, or there was an issue...
            return
        if result.get("status"):
            # letting us know the finished reporting went well
            # CLEAR WORKER HERE
            return
        # worker logic goes here... should just start and return,
        # using zmq.PUSH and zmq.PULL to monitor for finished statuses.
        method = message["result"]["method"]
        params = message["result"]["params"]
        task_id = message["result"]["id"]
        data = { "method": method, "params": params, "id": task_id }
        result = self.dispatcher.dispatch(data)
        response = {"method": "node.task_finished",
                    "params": {"id": task_id, "result": result["result"]}}
        logging.info("Sending message %s", response)
        self.broker_req_socket.send_json(response)


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
