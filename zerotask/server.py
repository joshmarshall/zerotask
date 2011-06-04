""" A generic ZeroMQ server which uses the poller for multiple sockets """

import zmq
import logging

class Server(object):
    """ The server base class """

    def __init__(self):
        self.poller = zmq.Poller()
        self.context = zmq.Context()
        self.callbacks = []
        self.break_loop = False

    def config(self):
        """ Overwrite in subclasses """
        pass

    def loop(self):
        """ Polls and fires callbacks based on sockets """
        self.setup()
        logging.info("Starting server...")
        while True:
            socks = dict(self.poller.poll())
            for socket, callback in self.callbacks:
                if socks.get(socket) == zmq.POLLIN:
                    data = socket.recv_json()
                    callback(data)
            if self.break_loop:
                break

    start = loop

    def add_callback(self, socket, callback):
        """ Adds a socket and callback to the poller and callbacks list """
        self.poller.register(socket, zmq.POLLIN)
        self.callbacks.append((socket, callback))

    def setup(self):
        """ Should be overwritten by child classes """
        pass

