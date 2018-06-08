Orchestra
=========

The Orchestra workflow DSL is the native DSL for the Orchestra workflow engine. This DSL is a
derivative of `OpenStack Mistral <https://docs.openstack.org/mistral/latest/user/wf_lang_v2.html>`_.

Workflow Model
--------------
The following is the list of attributes that makes up the workflow model. A workflow takes input,
perform a set of tasks in predefined order, and return output. The workflow model here is a
directed graph where the tasks are the nodes and the transitions and their condition between tasks
form the edges. The tasks that compose a workflow will be defined in the DSL as a dictionary named
`tasks` where the key and value is the task name and task model respectively. 

+-------------+-------------------------------------------------------------------+
| Attribute   | Description                                                       |
+=============+===================================================================+
| version     | The version of the spec being used in this workflow DSL.          |
+-------------+-------------------------------------------------------------------+
| description | The description of the workflow.                                  |
+-------------+-------------------------------------------------------------------+
| input       | A list of input arguments for this workflow.                      |
+-------------+-------------------------------------------------------------------+
| vars        | A list of variables defined for the scope of this workflow.       |
+-------------+-------------------------------------------------------------------+
| tasks       | A dictionary of tasks that defines the intent of this workflow.   |
+-------------+-------------------------------------------------------------------+
| output      | A list of variables defined as output for the workflow.           |
+-------------+-------------------------------------------------------------------+

.. literalinclude:: /examples/sequential.yaml
   :language: yaml
