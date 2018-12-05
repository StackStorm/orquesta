Changelog
=========

0.3
---

Added
~~~~~

* Add with items model to the orquesta task spec. (improvement)
* Add delay attribute to the orquesta task spec. (improvement)
* Add script to generate JSON schema from the workflow models. (improvement)
* Add command to make package for upload to pypi. (improvement)

Changed
~~~~~~~

* Allow workflow to output on other completed states such as error. (improvement)
* Allow multiple transition between two tasks. (improvement)
* Refactor finalize_context in task spec to get the transition precisely. (improvement)
* Refactor conductor log entries and methods. (improvement)
* Refactor rendering of task spec in conductor to support with items model. (improvement)

Fixed
~~~~~

* Fix state change when workflow output fails rendering. (bug fix)
* Fix vars and input that references app context. (bug fix)
* Convert strings in context to unicode to fix problems with mixing
  ascii and unicode strings in expressions. (bug fix)


0.2
---

Added
~~~~~

* Add pep8 checks on module imports and other coding styles. (improvement)
* Allow application to pass runtime context on workflow execution. (improvement)
* Log an error in the conductor when a failed execution event is received. (improvement)

Changed
~~~~~~~

* Rename project from orchestra to orquesta.
* Replace if else logic with state machines to handle state transition for
  task and workflow execution. (improvement)
* Refactor expression functions to not have to require the context argument. (improvement)
* Clean up conductor and mark some of the methods as private. (improvement)

Fixed
~~~~~

* Fix bug where current task is not set in the context when task spec is rendering. (bug fix)
* Fix bug where self looping task reference an outdated context. (bug fix)
* Fix bug where self looping task does not terminate. (bug fix)
