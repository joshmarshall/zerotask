""" The broker class, for dispatching and monitoring tasks """

import zerotask
from zerotask.server import Server
from zerotask.dispatcher import Dispatcher
import zmq
import logging
import random
import string
import optparse

def get_random_id():
    """ Generate a random id """
    return "".join([random.choice(string.letters) for i in range(20)])

class Broker(Server):
    """ The broker class, which dispatches and monitors jobs """

    def __init__(self, **kwargs):
        self.address = kwargs.get("address", "*")
        self.reply_port = kwargs.get("reply_port", 5555)
        self.publish_port = kwargs.get("publish_port", 5556)
        self.protocol = kwargs.get("protocol", "tcp")
        self.publish_socket = None
        self.reply_socket = None
        self.tasks = {} # need persistance / repl. later
        self.dispatcher = Dispatcher()
        Server.__init__(self)

    def setup(self):
        """ Creates the reply and publish sockets """
        base_uri = "%s://%s:%%s" % (self.protocol, self.address)
        publish_uri = base_uri % self.publish_port
        reply_uri = base_uri % self.reply_port
        self.publish_socket = self.context.socket(zmq.PUB)
        self.reply_socket = self.context.socket(zmq.REP)
        self.publish_socket.bind(publish_uri)
        self.reply_socket.bind(reply_uri)
        self.add_callback(self.reply_socket, self.dispatch)
        self.dispatcher.add_handler(self.new_task, "broker.new_task")
        self.dispatcher.add_handler(self.task_request, "node.task_request")
        self.dispatcher.add_handler(self.task_finished, "node.task_finished")
        self.dispatcher.add_handler(self.task_result, "broker.task_result")

    def dispatch(self, message):
        """ Parse methods and send to proper places """
        logging.info("Receiving message %s", message)
        method = message["method"] # bad, bad, bad...
        if self.dispatcher.has_handler(method):
            result = self.dispatcher.dispatch(message)
        else:
            result = {"error": {"message": "Unknown method", "code": -1}}
        logging.info("Sending message %s", result)
        self.reply_socket.send_json(result)

    def new_task(self, method, params):
        """ New task submitted """
        task_id = get_random_id() # throwing away initial id
        pub_message = {"method": "node.task_ready",
                       "params": dict(method=method, id=task_id)}
        self.publish_socket.send_json(pub_message)
        task = {"method": method, "params": params, "id": task_id}
        task["_status"] = zerotask.QUEUED
        self.tasks[task_id] = task
        response = { "id": task_id }
        return response

    def task_request(self, id):
        """ Receive a task request from a node """
        task = self.tasks.get(id)
        if task and task["_status"] == zerotask.QUEUED:
            result = task
            task["_status"] = zerotask.ASSIGNED
        else:
            result = {}
        return result

    def task_finished(self, result, id):
        """ Run task finished process """
        task = self.tasks.get(id)
        task["result"] = result
        task["_status"] = zerotask.FINISHED
        pub_msg = {"jsonrpc": "2.0", "method": "task.finished",
                   "params": {"id": id}}
        logging.info("Publishing message %s", pub_msg)
        self.publish_socket.send_json(pub_msg)
        return { "status": "ok" }

    def task_result(self, id):
        """ Retrieve a result and drop it from store """
        task = self.tasks.get(id)
        result = task["result"]
        del self.tasks[id]
        return result

def main():
    """ Start up a simple broker. """
    options = optparse.OptionParser()
    options.add_option("-r", "--request_port", dest="reply_port", type="int",
                       default=5555, help="the request socket port")
    options.add_option("-p", "--publish_port", dest="pub_port", type="int",
                       default=5556, help="the publish socket port")
    options.add_option("-a", "--address", dest="address",
                       default="*", help="the bind address")
    options.add_option("-l", "--loglevel", dest="loglevel",
                       default="WARNING", help="INFO|WARNING|ERROR")

    opts, args = options.parse_args()
    logging.getLogger().setLevel(getattr(logging, opts.loglevel))
    broker = Broker(reply_port=opts.reply_port, publish_port=opts.pub_port,
                    address=opts.address)
    logging.info("Starting broker request on port %d", opts.reply_port)
    logging.info("Starting broker publish on port %d", opts.pub_port)
    broker.start()

if __name__ == "__main__":
    main()
