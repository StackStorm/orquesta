Changelog
=========

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
