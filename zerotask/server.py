""" A generic ZeroMQ server which uses the poller for multiple sockets """

import zmq
import logging
from zerotask.dispatcher import Dispatcher

class Server(object):
    """ The server base class """

    namespace = ""

    def __init__(self, **kwargs):
        self.poller = zmq.Poller()
        self.context = zmq.Context()
        self.dispatcher = kwargs.get("dispatcher", Dispatcher.instance())
        self.callbacks = []
        self.break_loop = False
        self.setup()

    def loop(self):
        """ Polls and fires callbacks based on sockets """
        logging.info("Starting server %s...", self.namespace)
        try:
            while True:
                socks = dict(self.poller.poll())
                for socket, callback in self.callbacks:
                    if socks.get(socket) == zmq.POLLIN:
                        data = socket.recv_json()
                        callback(data)
                if self.break_loop:
                    break
        except KeyboardInterrupt:
            logging.info("Caught keyboard interrupt...")
            pass
        finally:
            logging.info("Shutting down server %s", self.namespace)
            self.teardown()
            return

    start = loop

    def add_callback(self, socket, callback):
        """ Adds a socket and callback to the poller and callbacks list """
        self.poller.register(socket, zmq.POLLIN)
        self.callbacks.append((socket, callback))

    def add_handler(self, method, name=None):
        """ just a wrapper for dispatcher.add_handler """
        if not name:
            # If name is not specified, generate one from 
            # server namespace and method name
            name = method.__name__
            if self.namespace:
                if not self.namespace.endswith("."):
                    name = "."+name
                name = self.namespace+name
        self.dispatcher.add_handler(method, name)

    def setup(self):
        """ Should be overwritten by child classes """
        pass

    def teardown(self):
        """ Should be overwritten by child classes """
        pass

