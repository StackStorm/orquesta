Orquesta Workflow Definition
============================

The Orquesta workflow DSL (Domain Specific Language) is the native DSL for the Orquesta workflow
engine. This DSL is derived from
`OpenStack Mistral <https://docs.openstack.org/mistral/latest/user/wf_lang_v2.html>`_.

Workflow Model
--------------
The following is the list of attributes that makes up the workflow model. A workflow takes input,
perform a set of tasks in predefined order, and returns output. The workflow model here is a
directed graph where the tasks are the nodes and the transitions and their condition between tasks
form the edges. The tasks that compose a workflow will be defined in the DSL as a dictionary named
``tasks`` where the key and value is the task name and task model respectively. 

+-------------+------------+-------------------------------------------------------------------+
| Attribute   | Required   | Description                                                       |
+=============+============+===================================================================+
| version     | Yes        | The version of the spec being used in this workflow DSL.          |
+-------------+------------+-------------------------------------------------------------------+
| description | No         | The description of the workflow.                                  |
+-------------+------------+-------------------------------------------------------------------+
| input       | No         | A list of input arguments for this workflow.                      |
+-------------+------------+-------------------------------------------------------------------+
| vars        | No         | A list of variables defined for the scope of this workflow.       |
+-------------+------------+-------------------------------------------------------------------+
| tasks       | Yes        | A dictionary of tasks that defines the intent of this workflow.   |
+-------------+------------+-------------------------------------------------------------------+
| output      | No         | A list of variables defined as output for the workflow.           |
+-------------+------------+-------------------------------------------------------------------+

The following is a simple workflow example that illustrates the various sections of the model:

.. code-block:: yaml

    version: 1.0

    description: A simple workflow.

    # A list of strings, assuming value will be provided at runtime or
    # key value pairs where value is the default value when value
    # is not provided at runtime.
    input:
      - arg1
      - arg2: abc

    # A list of key value pairs.
    vars:
      - var1: 123
      - var2: True
      - var3: null

    # A dictionary of task definition. The order of execution is
    # determined by inbound task transition and the condition of
    # the outbound transition.
    tasks:
      task1:
        action: core.noop
        next:
          - do: task2
      task2:
        action: core.noop

    # A list of key value pairs to output.
    output:
      - var3: <% ctx().arg1 %>
      - var4:
          var41: 456
          var42: def
      - var5:
          - 1.0
          - 2.0
          - 3.0

Task Model
----------

+-------------+-------------+-------------------------------------------------------------------+
| Attribute   | Required    | Description                                                       |
+=============+=============+===================================================================+
| delay       | No          | If specified, the number of seconds to delay the task execution.  |
+-------------+-------------+-------------------------------------------------------------------+
| join        | No          | If specified, sets up a barrier for a group of parallel branches. |
+-------------+-------------+-------------------------------------------------------------------+
| with        | No          | When given a list, execute the action for each item.              |
+-------------+-------------+-------------------------------------------------------------------+
| action      | No          | The fully qualified name of the action to be executed.            |
+-------------+-------------+-------------------------------------------------------------------+
| input       | No          | A dictionary of input arguments for the action execution.         |
+-------------+-------------+-------------------------------------------------------------------+
| retry       | No          | If specified, define requirements for task to be retried.         |
+-------------+-------------+-------------------------------------------------------------------+
| next        | No          | Define what happens after this task is completed.                 |
+-------------+-------------+-------------------------------------------------------------------+

As described above, the workflow is a directed graph where the tasks are the nodes and the
transitions with their criteria between tasks form the edges. The starting set of tasks for
the workflow are tasks with no inbound edges in the graph. On completion of the task, the next
set of tasks defined in the list of task transitions under ``next`` will be evaluated. Each task
transition will be represented as an outbound edge of the current task. When the criteria
specified in ``when`` of the transition is met, the next set of tasks under ``do`` will be invoked.
If there are no more outbound edges identified, then the workflow execution is complete.

The Orquesta workflow engine is designed to fail fast and terminate the execution of the
workflow when a task fails and no remediation for the failed task is defined. If the failed
task has no transition for that failure condition, then the workflow will stop as soon as
any in-progress tasks are completed. To be more specific, if there are multiple parallel branches
and a task failed in one branch with no transition defined for the failure, then the workflow
engine will wait for all currently running tasks in the other branches to finish and then
terminate the workflow. No other tasks from any branches will be queued or scheduled after the
workflow is terminated. This design allows users to quickly identify and address problems during
runtime without waiting for the other tasks in other branches to complete. The users can
quickly fix the problem and either rerun the workflow from the beginning or from where the
workflow failed.

.. note::
  The fail fast design of Orquesta is different to Mistral workflows, therefore when migrating
  from Mistral to Orquesta a re-design may be required.

Each task defines what **StackStorm** action to execute, the policies on action execution, and
what happens after the task completes. All of the variables defined and published up to this point
(aka context) are accessible to the task. At its simplest, the task executes the action and passes
any required input from the context to the action execution. On successful completion of the action
execution, the task is evaluated for completion. If criteria for transition is met, then the next
set of tasks is invoked, sequentially in the order of the transitions and tasks that are listed.

If more than one tasks transition to the same task and ``join`` is specified in the latter (i.e. the
task named ``barrier_task`` in the example below), then the task being transitioned into becomes a
barrier for the inbound task transitions. There will be only one instance of the barrier task. In
the workflow graph, there will be multiple inbound edges to the barrier node.

The following workflow definition illustrates the execution of parallel branches. The barrier task
will be blocked until all the parallel branches complete and reach it.

.. code-block:: yaml

    version: 1.0

    tasks:
      setup_task:
        # Run tasks in parallel
        next:
          - do:
              - parallel_task_1
              - parallel_task_2
              - parallel_task_3

      parallel_task_1:
        # Wait to run barrier_task after this
        action: core.noop
        next:
          - when: <% succeeded() %>
            do: barrier_task

      parallel_task_2:
        # Eventually run barrier_task
        action: core.noop
        next:
          - when: <% succeeded() %>
            do: intermediate_task

      intermediate_task:
        # Wait to run barrier_task after this
        action: core.noop
        next:
          - when: <% succeeded() %>
            do: barrier_task

      barrier_task:
        # Run after parallel_task_1, parallel_task_2, and intermediate_task have all finished
        join: all
        action: core.noop

      parallel_task_3:
        # Run immediately after setup_task, do NOT wait for barrier_task
        action: core.noop

The following is the corresponding workflow execution graph.

.. code-block:: none

    =---- time (not to scale) ---->

    setup_task --+
                 |
                 +-- parallel_task_1 --------------------------+
                 |                                             |
                 +-- parallel_task_2 --+                       |
                 |                     |                       |
                 |                     +-- intermediate_task --+
                 |                                             |
                 |                                             +-- barrier_task --+
                 |                                                                |
                 +-- parallel_task_3 ---------------------------------------------+
                                                                                  |
                                                                                  +-- [finish]

Conversely, if more than one tasks transition to the same task and ``join`` is **not** specified in
the latter, then the target task will be invoked immediately following the completion of the
previous task. There will be multiple instances of the target task. In the workflow graph, each
invocation of the target task will be its own branch with the inbound edge from the node of the
previous task.

In other words, if ``join: all`` was removed from the previous workflow, the ``barrier_task`` would
be run two different times, resulting in this execution graph:

.. code-block:: none

    =---- time (not to scale) ---->

    setup_task --+
                 |
                 +-- parallel_task_1 -------+
                 |                          |
                 |                          +-- barrier_task (1) ---------------------+
                 |                                                                    |
                 +-- parallel_task_2 --+                                              |
                 |                     |                                              |
                 |                     +-- intermediate_task --+                      |
                 |                                             |                      |
                 |                                             +-- barrier_task (2) --+
                 |                                                                    |
                 +-- parallel_task_3 -------------------------------------------------+
                                                                                      |
                                                                                      +-- [finish]

An alternative use case of join is to specify an integer value such as ``join: <integer>``
instead of ``join: all``. In this use case, the join is satisified when the number of tasks
transitioned into the join is greater than or equal to the value specified. Take the following
workflow definition below, which is a revised version of the workflow from previous example.
There are three tasks that run in parallel and will join at the barrier task. The join has a
value of 2 which means the join will be satisfied when two out of the three parallel tasks
complete and transition into the join. The ``barrier_task`` will immediately run when the
join criteria is satisfied.

.. code-block:: yaml

    version: 1.0

    tasks:
      setup_task:
        next:
          - do:
              - parallel_task_1
              - parallel_task_2
              - parallel_task_3

      parallel_task_1:
        action: core.noop
        next:
          - when: <% succeeded() %>
            do: barrier_task

      parallel_task_2:
        action: core.noop
        next:
          - when: <% succeeded() %>
            do: barrier_task

      parallel_task_3:
        action: core.noop
        next:
          - when: <% succeeded() %>
            do: barrier_task

      barrier_task:
        join: 2
        action: core.noop

The following is the corresponding workflow execution graph.

.. code-block:: none

    =---- time (not to scale) ---->

    setup_task --+
                 |
                 +-- parallel_task_1 --*
                 |                     *
                 +-- parallel_task_2 --*
                 |                     *
                 +-- parallel_task_3 --*
                                       *
                                       *-- barrier_task (only requires 2 of 3 tasks) --+
                                                                                       |
                                                                                       +-- [finish]

With Items Model
----------------

Use the ``with`` items section to process a list of items in a task. The task will iterate through
each item and request an action execution for each item. By default, all the items will be processed
at the same time. When ``concurrency`` is specified, the number of items up to the concurrency value
will be processed and the remaining items will be queued. When the action execution for an item is
completed, the next item in the list will be processed.

The task result is a list of the action execution results in the same order as the items. All action
executions must complete successfully for the task to reach a succeeded state. If one or more
action executions fail, then the task will result in a failed state.

When there's a request to cancel or pause the workflow, the task will be in a canceling or pausing
state respectively until all action executions in the process of being executed are completed. Once
these action executions are completed, the task will go to canceled or paused state respectively.
If concurrency for the task is specified and there are remaining items, no new action executions
will be requested. When a paused workflow resumes, the task will continue to process any remaining
items.

+-------------+-------------+-------------------------------------------------------------------+
| Attribute   | Required    | Description                                                       |
+=============+=============+===================================================================+
| items       | Yes         | The list of items to execute the action with.                     |
+-------------+-------------+-------------------------------------------------------------------+
| concurrency | No          | The number of items being processed concurrently.                 |
+-------------+-------------+-------------------------------------------------------------------+

The following is a simple example with a single list of items defined in a task. The task is given
a list of messages to echo. For an items list where no concurrency is required, there is a shorthand
notation to pass just the list directly to the ``with`` statement. The individual items can be
passed into the action as input for execution using the ``item`` function.

.. code-block:: yaml

    version: 1.0

    input:
      - messages

    tasks:
      task1:
        with: <% ctx(messages) %>
        action: core.echo message=<% item() %>

When concurrency is required, use the formal schema with ``items`` and ``concurrency`` instead
of the short hand notation for task definition.

.. code-block:: yaml

    version: 1.0

    input:
      - messages

    tasks:
      task1:
        with:
          items: <% ctx(messages) %>
          concurrency: 2
        action: core.echo message=<% item() %>

The item value can be named. The following example is the same workflow as the one above. Note
that the items are specified as ``message in <% ctx(messages) %>`` where the value of the item
is named "message" and can be referenced with the ``item`` function as ``item(message)``. The
value returned from ``item()`` in this case would be a dictionary like ``{"message": "value"}``.
The benefit is evident below when working with multiple lists of items.

.. code-block:: yaml

    version: 1.0

    input:
      - messages

    tasks:
      task1:
        with: message in <% ctx(messages) %>
        action: core.echo message=<% item(message) %>

For multiple lists of items, the lists need to be zipped first with the ``zip`` function and then
define the keys required to access the individual values in each item. In the example below, the
task will execute a specific command on a specific host. The hosts and commands are zipped via
``<% zip(ctx(hosts), ctx(commands)) %>`` and then the keys to access the values in each item is
defined as ``host, command in <% zip(ctx(hosts), ctx(commands)) %>``. Finally, when specifying the
input parameters for the action execution, host value is accessed via ``<% item(host) %>`` and the
command value is accessed via ``<% item(command) %>``.

.. code-block:: yaml

    version: 1.0

    input:
      - hosts
      - commands

    tasks:
      task1:
        with: host, command in <% zip(ctx(hosts), ctx(commands)) %>
        action: core.remote hosts=<% item(host) %> cmd=<% item(command) %>

Task Retry Model
----------------

If ``retry`` is defined, the task will be retried when the condition is met. The ``when`` condition
can be an expression that evaluates the status of the last action execution or its result. If the
number of retries are exhausted, then the final task state will be determined from the last action
execution for the task.

+-------------+-------------+-------------------------------------------------------------------+
| Attribute   | Required    | Description                                                       |
+=============+=============+===================================================================+
| when        | No          | The criteria defined as an expression required for retry.         |
+-------------+-------------+-------------------------------------------------------------------+
| count       | Yes         | The number of times to retry.                                     |
+-------------+-------------+-------------------------------------------------------------------+
| delay       | No          | The number of seconds to delay in between retries.                |
+-------------+-------------+-------------------------------------------------------------------+

In the following example, if task1 fails, it will be retried up to 3 times with 1 second delay.

.. code-block:: yaml

    version: 1.0

    input:
      - command

    tasks:
      task1:
        action: core.remote cmd=<% ctx().command %>
        retry:
          delay: 1
          count: 3
        next:
          - when: <% succeeded() %>
            do: task2

      task2:
        action: core.noop

In another example, task1 will be retried if the action execution returns status code other than
200. The task will be retried up to 3 times with no delay.

.. code-block:: yaml

    version: 1.0

    input:
      - url

    tasks:
      task1:
        action: core.http url=<% ctx().url %>
        retry:
          when: <% result().status_code != 200 %>
          count: 3
        next:
          - when: <% result().status_code = 200 %>
            do: task2

      task2:
        action: core.noop

Task Transition Model
---------------------

The ``next`` section is a list of task transitions to be evaluated after a task completes. A task is
completed if it either succeeded, failed, or canceled. The list of transitions will be processed in
the order they are defined. In the workflow graph, each task transition is one or more outbound
edges from the current task node. For each task transition, the ``when`` is the criteria that must
be met in order for transition. If ``when`` is not defined, then the default criteria is task
completion. When criteria is met, then ``publish`` can be defined to add new or update existing
variables from the result into the runtime workflow context. Finally, the list of tasks defined in
``do`` will be invoked in the order they are specified.

+-------------+-------------+-------------------------------------------------------------------+
| Attribute   | Required    | Description                                                       |
+=============+=============+===================================================================+
| when        | No          | The criteria defined as an expression required for transition.    |
+-------------+-------------+-------------------------------------------------------------------+
| publish     | No          | A list of key value pairs to be published into the context.       |
+-------------+-------------+-------------------------------------------------------------------+
| do          | No          | A next set of tasks to invoke when transition criteria is met.    |
+-------------+-------------+-------------------------------------------------------------------+

The following is a more complex workflow with branches and join and various ways to define
tasks and task transitions:

.. code-block:: yaml

    version: 1.0

    description: Calculates (a + b) * (c + d)

    input:
      - a: 0    # Defaults to value of 0 if input is not provided.
      - b: 0
      - c: 0
      - d: 0

    tasks:
      task1:
        # Fully qualified name (pack.name) for the action.
        action: math.add

        # Assign input arguments to the action from the context.
        input:
          operand1: <% ctx(a) %>
          operand2: <% ctx(b) %>

        # Specify what to run next after the task is completed.
        next:
          - # Specify the condition in YAQL or Jinja that is required
            # for this task to transition to the next set of tasks.
            when: <% succeeded() %>

            # Publish variables on task transition. This allows for
            # variables to be published based on the task state and
            # its result.
            publish:
              - msg: task1 done
              - ab: <% result() %>

            # List the tasks to run next. Each task will be invoked
            # sequentially. If more than one tasks transition to the
            # same task and a join is specified at the subsequent
            # task (i.e task1 and task2 transition to task3 in this
            # case), then the subsequent task becomes a barrier and
            # will be invoked when condition of prior tasks are met.
            do:
              - log
              - task3

      task2:
        # Short hand is supported for input arguments. Arguments can be
        # delimited either by space, comma, or semicolon.
        action: math.add operand1=<% ctx("c") %> operand2=<% ctx("d") %>
        next:
          - when: <% succeeded() %>

            # Short hand is supported for publishing variables. Variables
            # can be delimited either by space, comma, or semicolon.
            publish: msg="task2 done", cd=<% result() %>

            # Short hand with comma delimited list is supported.
            do: log, task3

      task3:
        # Join is specified for this task. This task will be invoked
        # when the condition of all inbound task transitions are met.
        join: all
        action: math.multiple operand1=<% ctx('ab') %> operand2=<% ctx('cd') %>
        next:
          - when: <% succeeded() %>
            publish: msg="task3 done" abcd=<% result() %>
            do: log

      # Define a reusable task to log progress. Although this task is
      # referenced by multiple tasks, since there is no join defined,
      # this task is not a barrier and will be invoked separately.
      log:
        action: core.log message=<% ctx(msg) %>

    output:
      - result: <% ctx().abcd %>

There are times when publish is required after a task completes but there are no more tasks
to execute next. In this case, a task transition can be defined without specifying the list
of ``do``. The following is a revision of the previous example:

.. code-block:: yaml

    version: 1.0

    description: Calculates (a + b) * (c + d)

    input:
      - a: 0    # Defaults to value of 0 if input is not provided.
      - b: 0
      - c: 0
      - d: 0

    tasks:
      task1:
        action: math.add operand1=<% ctx(a) %> operand2=<% ctx(b) %>
        next:
          - when: <% succeeded() %>
            publish: ab=<% result() %>
            do: task3

      task2:
        action: math.add operand1=<% ctx("c") %> operand2=<% ctx("d") %>
        next:
          - when: <% succeeded() %>
            publish: cd=<% result() %>
            do: task3

      task3:
        join: all
        action: math.multiple operand1=<% ctx('ab') %> operand2=<% ctx('cd') %>
        next:
          # After this task3 completes, it needs to publish the result
          # for output. Since there is no more tasks to execute afterward,
          # the do list is empty or not specified.
          - when: <% succeeded() %>
            publish: abcd=<% result() %>

    output:
      - result: <% ctx().abcd %>

The following example illustrates separate task transitions with different publishes
on different condition. After different message is published, both transition to the
same task to log the message. In the task transition for failure, an explicit
``fail`` command is specified to tell the workflow execution to fail. If the ``fail``
command is not specified, ``task2`` is considered a remediation task and the workflow
execution will succeed:

.. code-block:: yaml

    version: 1.0

    description: Send direct message to member

    input:
      - member
      - message

    tasks:
      task1:
        action: slack.post member=<% ctx(member) %> message=<% ctx(message) %>
        next:
          - when: <% succeeded() %>
            publish: msg="Successfully posted message."
            do:
              - task2
          - when: <% failed() %>
            publish: msg="Unable to post message due to error: <% result() %>"
            do:
              - task2
              - fail
      task2:
        action: core.log message=<% ctx(msg) %>


Engine Commands
---------------

The following is a list of engine commands with special meaning to the workflow engine.
When specified under ``do`` in the task transition, the engine will act accordingly. These
commands are also reserved words that cannot be used for task name.

+-------------+------------------------------------------------------------------------------------+
| Command     | Description                                                                        |
+=============+====================================================================================+
| continue    | Default value when ``do`` is not specified. The workflow engine will not alter the |
|             | previous task state and will continue to conduct the workflow execution. If the    |
|             | previous task state is one of the failure states, the conductor will continue and  |
|             | fail the workflow execution.                                                       |
+-------------+------------------------------------------------------------------------------------+
| fail        | The workflow engine will fail the workflow execution.                              |
+-------------+------------------------------------------------------------------------------------+
| noop        | The workflow engine will perform no operation given previous task state. If the    |
|             | previous task state is one of the failure states, the conductcor will ignore the   |
|             | task failure and assume a remediation has occurred.                                |
+-------------+------------------------------------------------------------------------------------+
| retry       | The workflow engine will retry the task up to 3 times with no delay.               |
+-------------+------------------------------------------------------------------------------------+

The following example illustrates the use of the default ``continue`` command to let the workflow
continue processing the task failure (or any other state) as normal. If ``task1`` fails, the second
task transition will publish the ``stderr`` and the conductor will continue with ``failed`` as the
final state of the workflow execution:

.. code-block:: yaml

    version: 1.0

    description: >
        A workflow example that illustrates error handling. By default if no task
        is specified under "do", the "continue" command is assumed. In this case
        where there is a task failure, the "continue" command will process the
        publish and then cascade the task failure to the workflow and the workflow
        execution will fail as a result.

    input:
      - cmd

    vars:
      - stdout: null
      - stderr: null

    tasks:
      task1:
        action: core.local cmd=<% ctx(cmd) %>
        next:
          - when: <% succeeded() %>
            publish: stdout=<% result().stdout %>
          - when: <% failed() %>
            publish: stderr=<% result().stderr %>

    output:
      - stdout: <% ctx(stdout) %>
      - stderr: <% ctx(stderr) %>

The following example is the same as the example above except the ``continue`` command is
explicit:

.. code-block:: yaml

    version: 1.0

    description: >
        A workflow example that illustrates error handling. In this case, the "continue"
        command is explicit. When there is a task failure, the "continue" command will
        process the publish and then cascade the task failure to the workflow and the
        workflow execution will fail as a result.

    input:
      - cmd

    vars:
      - stdout: null
      - stderr: null

    tasks:
      task1:
        action: core.local cmd=<% ctx(cmd) %>
        next:
          - when: <% succeeded() %>
            publish: stdout=<% result().stdout %>
            do: continue
          - when: <% failed() %>
            publish: stderr=<% result().stderr %>
            do: continue

    output:
      - stdout: <% ctx(stdout) %>
      - stderr: <% ctx(stderr) %>

The following example illustrates the use of the ``noop`` command to let the workflow
complete successfully even when there is a failure:

.. code-block:: yaml

    version: 1.0

    description: >
        A workflow example that illustrates error handling. When there is a task
        failure, the "noop" command specified will be treated as a remediation task
        and the conductor will succeed the workflow execution as normal.

    input:
      - cmd

    vars:
      - stdout: null
      - stderr: null

    tasks:
      task1:
        action: core.local cmd=<% ctx(cmd) %>
        next:
          - when: <% succeeded() %>
            publish: stdout=<% result().stdout %>
          - when: <% failed() %>
            publish: stderr=<% result().stderr %>
            do: noop

    output:
      - stdout: <% ctx(stdout) %>
      - stderr: <% ctx(stderr) %>


The following example is similar to the the one in previous section where it illustrates the use of
the ``fail`` command to explicitly fail the workflow. In this case where the failure of the http
call is communicated with a status number, a task transition is used to catch error when the
status code is not 200. An explicit ``fail`` command is used to signal the workflow execution
to fail:

.. code-block:: yaml

    version: 1.0

    description: A sample workflow to fetch data from a REST API.

    vars:
      - body: null

    tasks:
      task1:
        action: core.http url="https://api.xyz.com/objects"
        next:
          - when: <% succeeded() and result().status_code = 200 %>
            publish: body=<% result().body %>
          - when: <% succeeded() and result().status_code != 200 %>
            publish: body=<% result().body %>
            do: fail

    output:
      - body: <% ctx(body) %>

The example below illustrates the use of the ``retry`` command. The task will be retried if the
status code returned from the action execution is not 200. This is similar to using the more
explicit task retry model. The difference is that the retry command only retry up to 3 times with
no delay in between retries.

.. code-block:: yaml

    version: 1.0

    input:
      - url

    tasks:
      task1:
        action: core.http url=<% ctx().url %>
        next:
          - when: <% result().status_code != 200 %>
            do: retry
          - when: <% result().status_code = 200 %>
            do: task2

      task2:
        action: core.noop
