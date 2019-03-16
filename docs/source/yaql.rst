YAQL
====
`YAQL <http://yaql.readthedocs.io/en/latest/>`_ (Yet Another Query Language) is an OpenStack
project and allows for complex data querying and transformation. In the workflow definition,
YAQL expressions are wrapped inbetween ``<% %>`` like ``<% YAQL expression %>``.

Dictionaries
------------

To create a dictionary, use the ``dict`` function. For example, ``<% dict(a=>123, b=>true) %>``
returns ``{'a': 123, 'b': True}``. Let's say this dictionary is published to the context as
``dict1``. The keys function ``<% ctx(dict1).keys() %>`` returns ``['a', 'b']`` and
``<% ctx(dict1).values() %>`` returns the values ``[123, true]``. Concatenating dictionaries can
be done as ``<% dict(a=>123, b=>true) + dict(c=>xyz) %>`` which returns
``{'a': 123, 'b': True, 'c': 'xyz'}``.

A specific key value pair can be accessed by key name such as ``<% ctx(dict1).get(b) %>`` which
returns ``True``. Given the alternative ``<% ctx(dict1).get(b, false) %>``, if the key ``b`` does not
exist, then ``False`` will be returned by default.

Lists
-----

To create a list, use the ``list`` functions. For example, ``<% list(1, 2, 3) %>`` returns
``[1, 2, 3]`` and ``<% list(abc, def) %>`` returns ``['abc', 'def']``. List concatenation can be
done as ``<% list(abc, def) + list(ijk, xyz) %>`` which returns ``['abc', 'def', 'ijk', 'xyz']``.
If this list is published to the context as ``list1``, items can also be accessed via index such
as ``<% ctx(list1)[0] %>``, which returns ``abc``.

Queries
-------

Let's take the following context dictionary for example.

.. code-block:: json

    {
        "vms": [
            {
                "name": "vmweb1",
                "region": "us-east",
                "role": "web"
            },
            {
                "name": "vmdb1",
                "region": "us-east",
                "role": "db"
            },
            {
                "name": "vmweb2",
                "region": "us-west",
                "role": "web"
            },
            {
                "name": "vmdb2",
                "region": "us-west",
                "role": "db"
            }
        ]
    }

The following YAQL expressions are some sample queries that YAQL is capable of:

* ``<% ctx(vms).select($.name) %>`` returns the list of VM names
  ``['vmweb1', 'vmdb1', 'vmweb2', 'vmdb2']``.
* ``<% ctx(vms).select([$.name, $.role]) %>`` returns a list of names and roles as
  ``[['vmweb1', 'web'], ['vmdb1', 'db'], ['vmweb2', 'web'], ['vmdb2', 'db']]``.
* ``<% ctx(vms).select($.region).distinct() %>`` returns the distinct list of regions
  ``['us-east', 'us-west']``.
* ``<% ctx(vms).where($.region = 'us-east').select($.name) %>`` selects only the VMs in
  us-east ``['vmweb1', 'vmdb1']``.
* ``<% ctx(vms).where($.region = 'us-east' and $.role = 'web').select($.name) %>``
  selects only the web server in us-east ``['vmweb1']``.
* ``<% let(my_region => 'us-east', my_role => 'web') -> ctx(vms).where($.region = 
  $.my_region and $.role = $.my_role).select($.name) %>`` selects only the
  web server in us-east ``['vmweb1']``.

List to Dictionary
------------------

There are cases where it is easier to work with dictionaries rather than lists (e.g. random access
of a value with the key). Let's take the same list of VM records from above and convert it to a
dictionary where VM name is the key and the value is the record.

YAQL can convert a list of lists to a dictionary where each list contains the key and value. For
example, the expression ``<% dict(vms=>dict(ctx(vms).select([$.name, $]))) %>`` returns the
following dictionary:

.. code-block:: json

    {
        "vms": {
            "vmweb1": {
                "name": "vmweb1",
                "region": "us-east",
                "role": "web"
            },
            "vmdb1": {
                "name": "vmdb1",
                "region": "us-east",
                "role": "db"
            },
            "vmweb2": {
                "name": "vmweb2",
                "region": "us-west",
                "role": "web"
            },
            "vmdb2": {
                "name": "vmdb2",
                "region": "us-west",
                "role": "db"
            }
        }
    }

Built-in Functions
------------------

For the full list of built-in functions, see the `Standard Library section in YAQL docs
<https://yaql.readthedocs.io/en/latest/standard_library.html>`_. Some notable examples:

* ``float(value)`` converts value to float.
* ``int(value)`` converts value to integer.
* ``str(number)`` converts number to a string.
* ``len(list)`` and ``len(string)`` returns the length of the list and string respectively.
* ``max(a, b)`` returns the larger value between a and b.
* ``min(a, b)`` returns the smaller value between a and b.
* ``regex(expression).match(pattern)`` returns True if expression matches pattern.
* ``regex(expresssion).search(pattern)`` returns the first instance that matches the pattern.
* ``'some string'.toUpper()`` converts the string to all upper case.
* ``'some string'.toLower()`` converts the string to all lower case.
* ``['some', 'list'].contains(value)`` returns True if list contains value.
* ``"one, two, three, four".split(',').select(str($).trim())`` converts a comma separated
  string to an array, trimming each element.


Named Parameters in Function 
----------------------------

* Named parameters in function call must use the sign ``=>`` for assignment. Equal sign ``=`` in YAQL is used for evaluation and will result in the wrong value being passed for the parameter. For example, the built-in ``datetime`` function has parameters ``year, month, day, hour=0, minute=0, second=0, microsecond=0, offset=ZERO_TIMESPAN`` where year, month, and day are required parameters and the named parameters are optional. To assign value to hour, the function call will look like ``datetime(2020, 1, 1, hour=>12)``.

StackStorm Functions
--------------------

* ``st2kv('st2_key_id')`` queries the StackStorm datastore and returns the value for the given key. For
  example, the expression ``<% st2kv('system.shared_key_x') %>`` returns the value for a system
  scoped key named ``shared_key_x`` while the expression ``<% st2kv('my_key_y') %>`` returns the
  value for the user scoped key named ``my_key_y``. Please note that the key name should be in quotes
  otherwise YAQL treats a key name with a dot like ``system.shared_key_x`` as a dict access. The value
  can be encrypted in the StackStorm datastore. To decrypt the retrieved value, the input argument
  ``decrypt`` must be set to true such as ``st2kv('st2_key_id', decrypt=>true)``.
