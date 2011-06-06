""" The broker class, for dispatching and monitoring tasks """

import zerotask
from zerotask.server import Server
from zerotask.exceptions import JSONRPCError
from zerotask import jsonrpc
import zmq
import time
import logging
import optparse


TIMEOUT = 60 # one minute

class Broker(Server):
    """ The broker class, which dispatches and monitors jobs """

    namespace = "zerotask.broker"

    def __init__(self, **kwargs):
        self.address = kwargs.get("address", "*")
        self.reply_port = kwargs.get("reply_port", 5555)
        self.publish_port = kwargs.get("publish_port", 5556)
        self.protocol = kwargs.get("protocol", "tcp")
        self.publish_socket = None
        self.reply_socket = None
        self.tasks = {} # need persistance / repl. later
        self.clients = {} # need persistance / repl. later
        self.nodes = {} # need persistance / repl. later
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
        # Adding dispatcher handlers...
        self.add_handler(self.client_connect)
        self.add_handler(self.client_disconnect)
        self.add_handler(self.client_new_task)
        self.add_handler(self.client_task_status)
        self.add_handler(self.client_task_result)
        self.add_handler(self.node_connect)
        self.add_handler(self.node_disconnect)
        self.add_handler(self.node_task_request)
        self.add_handler(self.node_task_status)
        self.add_handler(self.node_task_finished)
        self.add_handler(self.node_task_failed)

    def dispatch(self, message):
        """ Parse methods and send to proper places """
        logging.info("Receiving message %s", message)
        result = self.dispatcher.dispatch(message)
        logging.info("Sending message %s", result)
        self.reply_socket.send_json(result)
    
    # Client methods
    # --------------

    def client_connect(self, client_id=None):
        """ Checks if a client id is valid and unused. """
        client_id = client_id or jsonrpc.get_random_id()
        client = self.clients.get(client_id)
        if not client:
            client = {}
            self.clients[client_id] = client
        client["activity"] = time.time()
        return client_id

    def client_disconnect(self, client_id):
        """ Deletes client from clients list """
        client = self.clients.get(client_id)
        if not client:
            raise JSONRPCError(jsonrpc.INVALID_CLIENT_ID)
        del self.clients[client_id]
        return True

    def client_new_task(self, client_id, method, params, task_id=None):
        """ Stores a CLIENTID-TASKID task structure and returns
        the task id to the client.
        It will create a task id if one is not provided.
        """
        if not self.clients.has_key(client_id):
            raise JSONRPCError(jsonrpc.INVALID_CLIENT_ID)
        task_id = task_id or jsonrpc.get_random_id()
        full_id = "%s-%s" % (client_id, task_id)
        if self.tasks.has_key(full_id):
            raise JSONRPCError(jsonrpc.INVALID_TASK_ID)
        task = self.tasks.get(full_id)
        announce_params = dict(method=method, task_id=full_id)
        announce_method = "zerotask.node.task_ready"
        pub_message = jsonrpc.request(method=announce_method,
                                      params=announce_params,
                                      id=None) # notification
        self.publish_socket.send_json(pub_message)
        task = dict(method=method,
                    params=params,
                    _status=zerotask.QUEUED,
                    id=task_id)
        self.tasks[full_id] = task
        return full_id

    def client_task_status(self, client_id, task_id):
        """ Retrieve a task status, if valid. """
        if not self.clients.has_key(client_id):
            raise JSONRPCError(jsonrpc.INVALID_CLIENT_ID)
        task = self.tasks.get(task_id)
        if not task:
            raise JSONRPCError(jsonrpc.INVALID_TASK_ID)
        return task["_status"]

    def client_task_result(self, client_id, task_id):
        """ Retrieve a result and drop it from store """
        if not self.clients.has_key(client_id):
            raise JSONRPCError(jsonrpc.INVALID_CLIENT_ID)
        task = self.tasks.get(task_id)
        if not task:
            raise JSONRPCError(jsonrpc.INVALID_TASK_ID)
        if not task["_status"] in (zerotask.FINISHED, zerotask.FAILED):
            raise JSONRPCError(jsonrpc.TASK_NOT_COMPLETE)
        result = task["result"]
        del self.tasks[task_id]
        return result

    # Node methods
    # ------------

    def node_connect(self, node_id=None):
        """ Adds a node to the node list, and returns node id """
        node_id = node_id or jsonrpc.get_random_id()
        node = self.nodes.get(node_id)
        if node and node['activity'] > time.time() - TIMEOUT:
            raise JSONRPCError(jsonrpc.INVALID_NODE_ID)
        if not node:
            node = {}
            self.nodes[node_id] = node
        self.nodes[node_id]['activity'] = time.time()
        return node_id

    def node_disconnect(self, node_id):
        """ Deletes a node entry from the node list """
        if not self.nodes.has_key(node_id):
            raise JSONRPCError(jsonrpc.INVALID_NODE_ID)
        del self.nodes[node_id]
        return True

    def node_task_request(self, node_id, task_id):
        """ Receive a task request from a node """
        if not self.nodes.has_key(node_id):
            raise JSONRPCError(jsonrpc.INVALID_NODE_ID)
        task = self.tasks.get(task_id)
        if not task:
            raise JSONRPCError(jsonrpc.INVALID_TASK_ID)
        if task and task["_status"] == zerotask.QUEUED:
            result = task
            task["_status"] = zerotask.ASSIGNED
            task["_node"] = node_id
            logging.info("Assigned task %s to node %s", task_id, node_id)
        else:
            # Task has already been assigned to a node
            result = None
        return result

    def node_task_status(self, node_id, task_id, status, **kwargs):
        """ Updates the task status """
        pass

    def node_task_finished(self, node_id, task_id, result):
        """ Run task finished process """
        if not self.nodes.has_key(node_id):
            raise JSONRPCError(jsonrpc.INVALID_NODE_ID)
        if not self.tasks.has_key(task_id):
            raise JSONRPCError(jsonrpc.INVALID_TASK_ID)
        task = self.tasks[task_id]
        task["result"] = result
        task["_status"] = zerotask.FINISHED
        announce_method = "zerotask.client.task_result_ready"
        announce_params = dict(task_id=task_id)
        pub_message = jsonrpc.request(method=announce_method,
                                      params=announce_params,
                                      id=None) # notification
        logging.info("Publishing message %s", pub_message)
        self.publish_socket.send_json(pub_message)
        return True

    def node_task_failed(self, node_id, task_id, error):
        """ Saves task error state and announces to client(s) """
        if not self.nodes.has_key(node_id):
            raise JSONRPCError(jsonrpc.INVALID_NODE_ID)
        if not self.tasks.has_key(task_id):
            raise JSONRPCError(jsonrpc.INVALID_TASK_ID)
        task = self.tasks[task_id]
        task["_status"] = zerotask.FAILED
        task["error"] = error
        announce_method = "zerotask.client.task_result_ready"
        announce_params = dict(task_id=task_id)
        pub_message = jsonrpc.request(method=announce_method,
                                      params=announce_params,
                                      id=None) # notification
        logging.info("Publishing message %s", pub_message)
        self.publish_socket.send_json(pub_message)
        return True


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
