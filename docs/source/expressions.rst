Expressions
===========

Expressions can be employed almost anywhere in the workflow definition, from the action name,
to assigning value to action input, to evaluating criteria in the task transition,
and data transformation when publishing variables.

Here are some of the things that an expression can accomplish:

* Select key-value pairs from a list of dictionaries.
* Filter the list where one or more fields match condition(s).
* Transform a list to dictionary or vice versa.
* Simple arithmetic.
* Evaluation of boolean logic.
* Any combination of select, filter, transform, and evaluate.

Applied to workflows, here are some use cases:

* Define what action to execute.
* Define input values to action execution.
* Define criteria for task transition.
* Publish variables to the runtime context on task completion.
* Reference variables from the runtime context.
* Define output values for workflow.

Types
-----
Orquesta currently supports `YAQL <http://yaql.readthedocs.io/en/latest/>`_ and
`Jinja <http://jinja.pocoo.org>`_ expressions. Both types of expressions can be used throughout the
workflow. Note that mixing of both YAQL and Jinja expressions in a single statement is not supported.
To tell Orquesta which expression is which, each type of expression should be encapsulated with
different symbols. For YAQL, the expression is wrapped in between ``<% YAQL expression %>``. For
Jinja, expression is wrapped in between ``{{ Jinja expression }}``. Code block using ``{% %}`` is
also supported for Jinja. The symbols ``{`` and ``}`` conflict with JSON and the entire Jinja
expression with the encapsulation should be single or double quoted.

Where it's used?
----------------

The following are attributes in the **Workflow** model that accept expressions:

+--------------+----------------------------------+
| Attributes   | Accept Expressions               |
+==============+==================================+
| version      | No                               |
+--------------+----------------------------------+
| description  | No                               |
+--------------+----------------------------------+
| input        | Yes, at each item                |
+--------------+----------------------------------+
| vars         | Yes, at each item                |
+--------------+----------------------------------+
| tasks        | See Task                         |
+--------------+----------------------------------+
| output       | Yes, at each item                |
+--------------+----------------------------------+

The following are attributes in the **Task** model that accept expressions:

+--------------+----------------------------------+
| Attributes   | Accept Expressions               |
+==============+==================================+
| delay        | Yes                              |
+--------------+----------------------------------+
| join         | No                               |
+--------------+----------------------------------+
| with         | Yes, see With Items              |
+--------------+----------------------------------+
| action       | Yes                              |
+--------------+----------------------------------+
| input        | Yes, entire dict or at each item |
+--------------+----------------------------------+
| next         | See Task Transition              |
+--------------+----------------------------------+

The following are attributes in the task **With Items** model that accept expressions:

+--------------+----------------------------------+
| Attributes   | Accept Expressions               |
+==============+==================================+
| items        | Yes                              |
+--------------+----------------------------------+
| concurrency  | Yes                              |
+--------------+----------------------------------+

The following are attributes in the **Task Transition** model that accept expressions:

+--------------+----------------------------------+
| Attributes   | Accept Expressions               |
+==============+==================================+
| when         | Yes                              |
+--------------+----------------------------------+
| publish      | Yes, at each item                |
+--------------+----------------------------------+
| do           | No                               |
+--------------+----------------------------------+

Usage
-----

The following sections go into more details on specific topics.

.. toctree::
   :maxdepth: 1

   Runtime Context <context>
   YAQL Expression <yaql>
   Jinja Expression <jinja>
