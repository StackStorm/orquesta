Jinja
=====
`Jinja <http://jinja.pocoo.org>`_ expressions are wrapped inbetween ``{{}}`` like
``{{ Jinja expression }}``. Code block using ``{% %}`` is also supported for Jinja. The symbols
``{`` and ``}`` conflict with JSON and the entire Jinja expression with the encapsulation
should be single or double quoted.

Built-in Filters
----------------

Jinja has a list of built-in filters to work with strings, dictionaries, lists, etc. Please
refer to the Jinja `documentation
<http://jinja.pocoo.org/docs/latest/templates/#list-of-builtin-filters>`_
for the list of available filters.

StackStorm Filters
------------------

* ``st2kv('st2_key_id')`` queries the StackStorm datastore and returns the value for the given key. For
  example, the expression ``{{ st2kv('system.shared_key_x') }}`` returns the value for a system
  scoped key named ``shared_key_x`` while the expression ``{{ st2kv('my_key_y') }}`` returns the
  value for the user scoped key named ``my_key_y``. Please note that the key name should be in quotes
  otherwise YAQL treats a key name with a dot like ``system.shared_key_x`` as a dict access. The value
  can be encrypted in the StackStorm datastore. To decrypt the retrieved value, the input argument
  ``decrypt`` must be set to true such as ``st2kv('st2_key_id', decrypt=true)``.
