ZeroTask
========

Intended to be a simple, extensible task system inspired by Celery but using only ZeroMQ+JSON-RPC (and eventually some kind of result store like Redis.)

Right now this is a really crappy prototype. I mean, really crappy. *really*

Concepts
--------
Terms:
* Dispatcher - Takes a JSON-RPC request in the current context and fires off handlers.
* Server - A collection of polled ZMQ sockets with callbacks.
* Broker - The dude responsible for handing out tasks and receiving results.
* Node - The dude responsible for doing all the work.
* Client - The dude requesting the work.
* Task - A function added to the dispatcher with an @task wrapper

A Node and a Broker are both subclasses of Server that use the local context Dispatcher. (You can also hand in your own dispatcher if you just want to be cool like that.)

A full task workflow needs at minimum one Node, one Broker, and one Client. 

Nodes
-----
A simple node example looks like:

    from zerotask.node import Node
    from zerotask.task import task

    @task
    def echo(foo):
        return foo

    Node().start()

You use @task to wrap a function and add it to the current dispatcher. This is actually how the Broker works too, which I'll get to in a second. Geez, be patient.

If you just want to play with a simple Node that adds and subtracts, run:

    python -m zerotask.node

Adding a -h will spit out some simple options.

If you run a node by its lonesome, it will just sit there patiently, but a Node does not expose any sockets itself -- instead, it binds to a Broker. Without a broker you can't do anything.

Brokers
-------
A broker exposes a request socket and a publish socket. The request socket is
what the Node(s) and Client(s) use to submit and request Tasks, get and report statuses, etc. The publish socket is for alerting the Node(s) that new Tasks are available, 

To fire up a Broker, you can just call the module:

    python -m zerotask.broker

If you want to see a few of the options, just run:

    python -m zerotask.broker -h

If you want to add custom Broker tasks (actually UTILIZING custom Broker tasks is beyond this scope) you can do something like:

    from zerotask.broker import Broker
    from zerotask.task import task

    @task(name="broker.new_hotness")
    def new_hotness():
        return 42

    Broker().start()

Now you are ready to use a Client.

Clients
-------
A client just connects to a Broker and sends / receives tasks. 

To use a Client, do the following:

    from zerotask.client import Client
    client = Client("tcp://127.0.0.1:5555")
    print client.add(5, 6)
    # Should print 11

Right now, it's just a simple blocking example. At some point we'll add an AsyncClient, and maybe some day some abstractions like MapReduce. Personally, I think adding async hooks for Tornado, etc. would be pretty sweet too.

I would LIKE the AsyncClient to operate like this:

    from zerotask.client import AsyncClient

    client = AsyncClient("tcp://127.0.0.1:5555")
    task = client.add(5, 6)
    print task.status() # should be "QUEUED", "RUNNING", etc.
    result = task.result() # or task.wait()
    print result # once again, 11

... but you know, I actually have to write all that first.
