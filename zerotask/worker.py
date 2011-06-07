""" The Worker class (down with the bourgeoisie?) """

import zmq
import logging
from zerotask.server import Server

class Worker(Server):
    """ This class handles all the work in a separate process,
    which is started and monitored by the parent node.
    """
    
    namespace = "zerotask.worker"

    def __init__(self, queue, result, **kwargs):
        self.name = kwargs.get("name", "worker")
        self.queue_uri = "ipc://%s" % queue
        self.result_uri = "ipc://%s" % result
        Server.__init__(self, **kwargs)

    def setup(self):
        logging.info("Worker %s listening on %s", self.name, self.queue_uri)
        logging.info("Worker %s posting to %s", self.name, self.result_uri)
        self._push_socket = self.context.socket(zmq.PUSH)
        self._push_socket.connect(self.result_uri)
        self._pull_socket = self.context.socket(zmq.PULL)
        self._pull_socket.connect(self.queue_uri)
        self.add_callback(self._pull_socket, self.receive_task)
        
    def receive_task(self, task):
        """ Dispatches task """
        logging.info("Received new task %s", task)
        result = self.dispatcher.dispatch(task)
        logging.info("Sending task result %s", result)
        self._push_socket.send_json(result)
