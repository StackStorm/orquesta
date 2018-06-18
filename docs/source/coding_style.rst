Coding Style
------------

* We follow `PEP8 Python Style Guide <https://www.python.org/dev/peps/pep-0008/>`_.
* We use the `hacking <https://pypi.org/project/hacking/>`_ flake8 plugins for additional checks.
* Use 4 spaces for a tab.
* Use 100 characters in a line.
* Make sure edited files don't contain any trailing whitespace.
* Make sure that all the source files contain an Apache 2.0 license header.
  See an existing python file for example.
* Be consistent with existing style of the file(s) being edited.
* Run ``tox -epep8`` to ensure code changes don't break any style rules.
