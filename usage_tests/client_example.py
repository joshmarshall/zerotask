""" This is just a pseudo-session in the way I would LIKE it to work. :) """

import zerotask
from zerotask.client import Client
from zerotask.exception import UnknownMethod, TaskError


# synchronous examples
client = Client() # connects to local broker on port 5555
assert(client.add(4, 6) == 10)
client = Client("tcp://some.address:5555")
assert(client.add(5, 6) == 11)
result = client.namespaces.are.awesome.foo()
assert(result == "bar")

assertRaises(UnknownMethod, client.some_misspelled_func)
assertRaises(TaskError, client.some_broken_func)

# Async examples
from zerotask.client import AsyncClient

client = AsyncClient("tcp://some.address:5555")
waiter1 = client.add(5, 6)
waiter2 = client.sleep(100)
waiter3 = client.add(100, 500) # this one takes longer because it's bigger! :)

result1 = waiter1.wait()
assert(result1 == 11)

status = waiter2.status()
assert(status == zerotask.RUNNING)

result2 = waiter2.wait()
# some time later...
assert(result2 == True) # or whatever else sleep() might return??

def callback(result):
    print "Woot!"

waiter3.callback(callback)

# we go on our merry way...
# and eventually "Woot!" should be printed.
# also, this should happen EVEN IF callback is added AFTER result returns


# and the big daddy pipe dream...
from zerotask.client import AsyncMapReduce

def read():
    import urllib
    data = urllib.urlopen("http://big.data.com/hugefile")
    for line in data:
        yield line

def mapfunc(line):
    for word in line.split(" "):
        yield word, 1

def reducefunc(key, values):
    count = 0
    for i in values:
        count += i
    return count

def write(result):
    final = {}
    for key, value in result:
        final[key] = value
    return final

amr = AsyncMapReduce("tcp://some.address:5555")
# should marshal all functions
amr.call(read=read, map=mapfunc, reduce=reducefunc, write=write)
result = amr.wait() # or .callback(), etc.
# ...and result should be whatever is returned from write.
