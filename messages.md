Message Spec
============
These are methods that each server should understand. 


Broker
------

zerotask.broker.client_connect([client_id]) ->
    returns the client id if successful (generates one if necessary)
    returns an error if the client id is taken or invalid

zerotask.broker.client_disconnect(client_id) ->
    returns True if the client disconnect stored properly
    returns an error if the client id is invalid or error storing disconnect

zerotask.broker.client_new_task(client_id, method, params, [task_id]) ->
    returns the task id if successful
    returns an error if the task id is taken or invalid

zerotask.broker.client_task_status(client_id, task_id) ->
    returns the task status if valid
    returns an error if the task id is unknown

zerotask.broker.client_task_result(client_id, task_id) ->
    returns the task result if valid
    returns an error if task id is unknown

zerotask.broker.node_connect([node_id]) ->
    returns the node id if successful (generates one if necessary)
    returns an error if node id is taken or invalid

zerotask.broker.node_heartbeat(node_id) ->
    verifies the node is still alive
    returns True if valid node
    returns an error if node id is invalid

zerotask.broker.node_disconnect(node_id) ->
    returns True if successful
    returns an error if node id is invalid

zerotask.broker.node_task_request(task_id) ->
    returns the {"method":METHOD, "params": PARAMS, "id": TASKID} if valid and
    unassigned
    returns {} if assigned
    returns an error if the task id is invalid or assigned

zerotask.broker.task_status(node_id, task_id, status, **kwargs):
    stores status and kwargs for task


zerotask.broker.node_task_finished(task_id, task_result) ->
    returns True if task result is stored properly
    returns an error if the task id is invalid or error storing result

zerotask.broker.node_task_failed(task_id, error) ->
    (error is a JSON-RPC error dictionary with "code" and "message")
    returns True if task error is stored properly
    returns an error if the task id is invalid or error storing error


Node
----
The node really only catches published messages from the broker.

zerotask.node.broker_new_task(method, task_id) ->
    Node should fire a zerotask.broker.node_task_request(task_id) to broker

zerotask.node.broker_node_status() ->
    Node should fire a zerotask.broker.node_status() to broker

