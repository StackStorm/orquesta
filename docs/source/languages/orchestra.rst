Orchestra Workflow Definition
=============================

The Orchestra workflow DSL (Domain Specific Language) is the native DSL for the Orchestra workflow
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
| join        | No          | If specified, sets up a barrier for a group of parallel branches. |
+-------------+-------------+-------------------------------------------------------------------+
| action      | No          | The fully qualified name of the action to be executed.            |
+-------------+-------------+-------------------------------------------------------------------+
| input       | No          | A dictionary of input arguments for the action execution.         |
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

Each task defines what **StackStorm** action to execute, the policies on action execution, and
what happens after the task completes. All of the variables defined and published up to this point
(aka context) are accessible to the task. At its simplest, the task executes the action and passes any
required input from the context to the action execution. On successful completion of the action
execution, the task is evaluated for completion. If criteria for transition is met, then the next
set of tasks is invoked, sequentially in the order of the transitions and tasks that are listed.

If more than one tasks transition to the same task and ``join`` is specified in the latter (i.e. the
task named "task3" in the example below), then the task being transitioned into becomes a barrier
for the inbound task transitions. There will be only one instance of the barrier task. In the
workflow graph, there will be multiple inbound edges to the barrier node.

Conversely, if more than one tasks transition to the same task and ``join`` is **not** specified in
the latter (i.e. the task named "log" in the example below), then the target task will be invoked
immediately following the completion of the previous task. There will be multiple instances of the
target task. In the workflow graph, each invocation of the target task will be its own branch with
the inbound edge from the node of the previous task.

Task Transition Model
---------------------

The ``next`` section is a list of task transitions to be evaluated after a task completes. A task is
completed if it either succeeded, failed, or canceled. The list of transitions will be processed in
the order they are defined. In the workflow graph, each task transition is one or more outbound
edges from the current task node. For each task transition, the ``when`` is the criteria that must be
met in order for transition. If ``when`` is not defined, then the default criteria is task completion.
When criteria is met, then ``publish`` can be defined to add new or update existing variables from the
result into the runtime workflow context. Finally, the list of tasks defined in ``do`` will be invoked
in the order they are specified.

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
              msg: task1 done
              ab: <% result() %>

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
on different condition:

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
            do: task2
          - when: <% failed() %>
            publish: msg="Unable to post message due to error: <% result() %>"
            do: task2
      task2:
        action: core.log message=<% ctx(msg) %>


Engine Commands
---------------

The following is a list of engine commands with special meaning to the workflow engine.
When specified under ``do`` in the task transition, the engine will act accordingly. These
commands are also reserved words that cannot be used for task name.

+-------------+-------------------------------------------------------------------+
| Command     | Description                                                       |
+=============+===================================================================+
| noop        | No operation or do not execute anything else.                     |
+-------------+-------------------------------------------------------------------+
| fail        | Fails the workflow execution.                                     |
+-------------+-------------------------------------------------------------------+

The following example illustrates the use of the ``fail`` command:

.. code-block:: yaml

    version: 1.0

    description: >
        A workflow example that illustrates error handling. By default
        when any task fails, the notify_on_error task will be executed
        and the workflow will transition to the failed state.

    input:
      - cmd

    tasks:
      task1:
        action: core.local cmd=<% ctx(cmd) %>
        next:
          - when: <% succeeded() %>
            publish: stdout=<% result().stdout %>
          - when: <% failed() %>
            publish: stderr=<% result().stderr %>
            do: notify_on_error
      notify_on_error:
        action: core.echo message=<% ctx(stderr) %>
        next:
          # The fail specified here tells the workflow to go into
          # failed state on completion of the notify_on_error task.
          - do: fail

    output:
      - result: <% $.stdout %>
