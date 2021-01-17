Getting Started
===============

Project Structure
^^^^^^^^^^^^^^^^^

Let's get started by walking thru the project structure and its contents.

::

    project
    |-- CHANGELOG.rst
    |-- LICENSE
    |-- Makefile
    |-- README.md
    |-- docs
    |-- orquesta
    |   |-- composers           # Plugins for composing a workflow spec into a graph.
    |   |   |-- native.py       # The plugin for the Orquesta workflow definition.
    |   |-- expressions         # Plugins for expressions used in the workflow definition.
    |   |   |-- functions       # Plugins for custom functions used in the expressions.
    |   |   |-- jinja.py        # The plugin to evaluate Jinja expressions.
    |   |   |-- yql.py          # The plugin to evaluate YAQL expressions.
    |   |-- specs               # Plugins for the workflow language specs.
    |   |   |-- native          # Models for the Orquesta workflow language.
    |   |   |-- types.py        # Base class types used in spec modeling.
    |   |-- tests
    |   |   |-- fixtures        # Reusable workflow definition used in unit tests.
    |   |   |-- hacking         # Plugins for custom code style checking rules.
    |   |   |-- unit            # Modules for all the unit tests. 
    |   |-- utils               # Modules for various reuseable utility functions.
    |   |-- conducting.py       # Module for the workflow engine that conducts the execution.
    |   |-- constants.py        # Module for project constants and values.
    |   |-- events.py           # Module that defines events for workflow execution.
    |   |-- exceptions.py       # Module that defines custom exception types.
    |   |-- graphing.py         # Module for the workflow execution graph.
    |   |-- machines.py         # Module for workflow and task state machines to process events.
    |   |-- statuses.py         # Module that defines status values for workflow execution.
    |-- requirements*.txt       # Files that list the project dependencies.
    |-- setup.py                # Project info and entry points where plugins are registered.
    |-- tox.ini                 # Configuration file for the tox command.


Coding Style
^^^^^^^^^^^^

* We follow `PEP8 Python Style Guide <https://www.python.org/dev/peps/pep-0008/>`_.
* We use the `hacking <https://pypi.org/project/hacking/>`_ flake8 plugins for additional checks
  and additional flake8 plugins are located under ``./orquesta/tests/hacking``.
* Use 4 spaces for a tab.
* Use 100 characters in a line.
* Make sure edited files don't contain any trailing whitespace.
* Make sure that all the source files contain an Apache 2.0 license header.
  See an existing python file for example.
* Be consistent with existing style of the file(s) being edited.
* Run ``tox -epep8`` to ensure code changes don't break any style rules.


Running Tests
^^^^^^^^^^^^^

At the root of the project directory, run the command ``tox`` to run all the unit tests for various
python versions, run pep8 to check code styles, and check docs for errors. The configuration for
tox is located in the ``tox.ini`` file. Alternatively, individual environments can be run by
passing the ``-e`` arg.  For example, the command ``tox -epy36`` runs the python 3.6 unit tests and
the command ``tox -epep8`` runs the pylint and flake8 checks.

It is possible to run a single unit test or tests within a single test module. Let use the test
module ``./orquesta/tests/unit/conducting/test_workflow_conductor.py`` as an example. The following
is examples for creating the python virtualenv and running either all the tests in a module or
running only a single test within the module.

.. code-block:: bash

    # Make and activate the virtualenv.
    $ make venv
    $ . ./.venv/bin/activate

    # Run all the tests in module ./orquesta/tests/unit/conducting/test_workflow_conductor.py.
    $ python -m unittest orquesta.tests.unit.conducting.test_workflow_conductor

    # Run a single test such as test_init in the WorkflowConductorTest class.
    $ python -m unittest orquesta.tests.unit.conducting.test_workflow_conductor.WorkflowConductorTest.test_init
