StackStorm Integration
======================

Testing Changes
^^^^^^^^^^^^^^^

To test Orquesta changes on StackStorm, update the Orquesta version in appropriate requirement
files, rebuild the StackStorm virtualenv, and run Orquesta related tests.

1. Push feature/change branch to the Orquesta repo.
2. Clone the StackStorm repo and checkout appropriate branch.
3. Run the following command to update the Orquesta in the StackStorm requirement files.

.. code-block:: bash

    # Replace the value of my-dev-branch with the name of the feature branch.
    $ export ORQUESTA_BRANCH="my-dev-branch" 
    $ sed -i -e "s/\(orquesta.git@\).*\(#egg=orquesta\)/\1${ORQUESTA_BRANCH}\2/" ./st2common/in-requirements.txt
    $ sed -i -e "s/\(orquesta.git@\).*\(#egg=orquesta\)/\1${ORQUESTA_BRANCH}\2/" ./contrib/runners/orquesta_runner/in-requirements.txt

4. Rebuild the StackStorm virtualenv. Take note that auto-generation tool will update related
   requirements.txt files.

.. code-block:: bash

    # Remove and rebuild the StackStorm virtualenv.
    $ make distclean
    $ make requirements

5. Run the Orquesta runner unit tests. The unit tests are located under the directory
   ``./contrib/runners/orquesta_runner/tests/unit``.

.. code-block:: bash

    # Run unit tests for the Orquesta runner.
    $ . virtualenv/bin/activate
    $ nosetests --rednose --immediate --with-parallel -s -v contrib/runners/orquesta_runner/tests/unit

6. Run the Orquesta integration tests. The integration tests are located under the directory
   ``./st2tests/integration/orquesta``.

.. code-block:: bash

    # Launch StackStorm screen sessions and run the Orquesta integration tests.
    $ ./tools/launchdev.sh start -x
    $ make orquesta-itests


Commiting Changes
^^^^^^^^^^^^^^^^^

Once the Orquesta feature branch is completed and merged into Orquesta master branch, update the
Orquesta version in appropriate StackStorm requirement files.

1. Merge feature/change PR to the Orquesta master branch.
2. Clone the StackStorm repo and create new feature branch.
3. Run the following command to update the Orquesta in the StackStorm requirement files.

.. code-block:: bash

    # Replace the value of new-commit with the latest commit in Orquesta containing the change(s).
    $ export ORQUESTA_COMMIT="new-commit" 
    $ sed -i -e "s/\(orquesta.git@\).*\(#egg=orquesta\)/\1${ORQUESTA_COMMIT}\2/" ./st2common/in-requirements.txt
    $ sed -i -e "s/\(orquesta.git@\).*\(#egg=orquesta\)/\1${ORQUESTA_COMMIT}\2/" ./contrib/runners/orquesta_runner/in-requirements.txt

4. Rebuild the StackStorm virtualenv. Take note that auto-generation tool will update related
   requirements.txt files.

.. code-block:: bash

    # Remove and rebuild the StackStorm virtualenv.
    $ make distclean
    $ make requirements

5. Run the Orquesta runner unit tests. The unit tests are located under the directory
   ``./contrib/runners/orquesta_runner/tests/unit``.

.. code-block:: bash

    # Run unit tests for the Orquesta runner.
    $ . virtualenv/bin/activate
    $ nosetests --rednose --immediate --with-parallel -s -v contrib/runners/orquesta_runner/tests/unit

6. Run the Orquesta integration tests. The integration tests are located under the directory
   ``./st2tests/integration/orquesta``.

.. code-block:: bash

    # Launch StackStorm screen sessions and run the Orquesta integration tests.
    $ ./tools/launchdev.sh start -x
    $ make orquesta-itests 

7. Create a PR in the StackStorm repo for the requirement changes.
