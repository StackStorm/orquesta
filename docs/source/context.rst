Workflow Runtime Context
========================

At runtime, the workflow execution maintain a context dictionary that manages assigned variables.
In the workflow definition, there are several location where variables can be assigned into the
context. These locations are ``input``, ``vars``, and ``output`` in the workflow model and
``publish`` in the task transition model. The order of the variables being assigned into the
context dictionary at runtime goes from workflow ``input`` and workflow ``vars`` at the start of
workflow execution, to ``publish`` on each task completion, then finally ``output`` on workflow
completion. 

Once a variable is assigned into the context dictionary, it can be referenced by a
custom function named ``ctx``. The ``ctx`` function takes the variable name as argument like
``ctx(foobar)`` or returns the entire dictionary if no argument is provided. This can be
referenced by dot notation - e.g. ``ctx().foobar``.

Basics
------

Let's revisit the workflow example we've used before. This example is using YAQL expressions.
The ``ctx`` function is also available to Jinja expressions. This workflow calculates a simple math
equation on inputs ``a``, ``b``, ``c``, and ``d``. The workflow input is provided on invocation.
If input is not provided at runtime, a default value is assigned. In this case, all the variables
will be assigned a value of 0.

These variables are then assigned into the context dictionary. After variables from ``input`` are assigned,
then the variables from ``vars`` will be assigned. In the example, the workflow executes the addition
in ``task1`` and ``task2`` in parallel. When these tasks complete, ``task3`` multiplies the results
from ``task1`` and ``task2``. 

In the ``task1`` definition, please note the different way the variables ``a`` and ``b`` are assigned to
``operand1`` and ``operand2``. Note that the variable can be returned by ``ctx`` directly when the name
of the variable is provided as input argument, as in the case ``ctx(a)``, or ``ctx`` can return the
entire dictionary, and the variable is accessed via dot notation, e.g. ``ctx().b``. 

As shown in ``task2``, the input argument to ``ctx`` can also be single quoted or double quoted. For
Jinja expressions, single or double quotes are required because Jinja will treat any unquoted literals
as variables.

When ``task1`` and ``task2`` completes, each task will ``publish`` the result into the context dictionary.
Since these tasks run in parallel, the task that completes first will write to the context dictionary
first. A race between parallel tasks is possible and Orquesta will handle the race condition. It is
best practice to avoid using the same variable name in parallel branches that converge downstream.

.. code-block:: yaml

    version: 1.0

    description: Calculates (a + b) * (c + d)

    input:
      - a: 0    # Defaults to value of 0 if input is not provided.
      - b: 0
      - c: 0
      - d: 0

    vars:
      - ab: 0
      - cd: 0
      - abcd: 0

    tasks:
      task1:
        action: math.add
        input:
          operand1: <% ctx(a) %>
          operand2: <% ctx().b %>
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
          - when: <% succeeded() %>
            publish: abcd=<% result() %>

    output:
      - result: <% ctx().abcd %>

Assignment Order
----------------

In the workflow defintion where variables are assigned into the context dictionary such as
``input``, ``vars``, ``publish``, and ``output``, the variables are defined as a list of key value
pairs. Orquesta will evaluate the assignment and associated expression in the order that the
variables are listed. Variables that have already been assigned earlier in the list are immediately
available for reference. Take the following workflow as an example, the input variable ``x`` is
immediately available for reference in the assignment of ``y``:

.. code-block:: yaml

    version: 1.0

    input:
      - x
      - y: <% ctx(x) %>
      - z: <% ctx(y) %>

    tasks:
      task1:
        action: core.echo message=<% ctx(z) %>

Assignment Scope
----------------

In a workflow with parallel branches, the context dictionary is scoped to each branch and merged
when the branches converge with ``join``. So let's say a variable is defined in the workflow
``input`` or ``vars`` and the workflow execution diverges into multiple branches. If task(s) from
each branch publishes to the same variable, the change is not global and is only made to the local
branch. Therefore, for each branch, the variable will have the new value from when it was assigned.
When the two branches converge, the local context dictionaries of these branches will also merge.
For variables with the same name between the context dictionaries, the branch that writes last will
overwrite the value in the merged context dictionary.

In the following example, there are two branches with one that starts at ``task1`` and another that
starts at ``task2``. The branch that starts with ``task2`` will take longer to complete because of
the explicit sleep. Both branch publishes to an existing variable ``x`` in the context dictionary.
Since branch 1 will complete first, ``x=123`` will be written to the context dictionary for
``task4`` first. When branch 2 completes, it will overwrite with ``x=789``:

.. code-block:: yaml

    version: 1.0

    vars:
      - x

    tasks:
      # Branch 1
      task1:
        action: core.noop
        next:
          - publish: x=123
            do: task4

      # Branch 2
      task2:
        action: core.sleep delay=3
        next:
          - do: task3
      task3:
        action: core.noop
        next:
          - publish: x=789
            do: task4

      # Converge branch 1 and 2
      task4:
        join: all
        action: core.noop

Dynamic Action Execution
------------------------

Sometimes the name of the action to execute is not known when writing a workflow. Instead,
the name of the action needs to be determined dynamically at runtime. This is possible with
Orquesta by placing an expression in the ``action`` property of a ``task``. The expression
for the ``action`` property will be rendered first, then the action will be executed given
the other properties of the task. Example:

.. code-block:: yaml

    version: 1.0

    input:
      - dynamic_action
      - data

    tasks:
      task1:
        action: "{{ ctx().dynamic_action }}"
        input:
          x: "{{ ctx().data }}"

In the example above, the workflow takes a parameter ``dynamic_action``, this is a string of
the full action ref (``<pack>.<action>``, ex: ``core.local``) to execute.

Additionally, action inputs can be dynamically assigned using expresssions:

.. code-block:: yaml

    version: 1.0

    input:
      - dynamic_action
      - dynamic_input

    tasks:
      task1:
        action: "{{ ctx().dynamic_action }}"
        input: "{{ ctx().dynamic_input }}"

In the example above, the workflow adds a parameter ``dynamic_input`` of type ``object``.
The ``dynamic_input`` is then assigned directly to the tasks's ``input``, allowing any
combination of parameters to be passed to the dynamic action. One might invoke this workflow
using the following:

.. code-block:: sh

    st2 run default.dynamic_workflow dynamic_action='core.local' dynamic_input='{"cmd": "date"}'

This is effectively the same as executing:

.. code-block:: sh

    st2 run core.local cmd=date

