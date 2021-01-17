# Orquesta

Orquesta is a graph based workflow engine designed specifically for
[StackStorm](https://github.com/StackStorm/st2). As a building block, Orquesta does not include
all the parts such as messaging, persistence, and locking required to run as a service.

The engine consists of the workflow models that are decomposed from the language spec, the composer
that composes the execution graph from the workflow models, and the conductor that directs the
execution of the workflow using the graph.

A workflow definition is a structured YAML file that describes the intent of the workflow. A
workflow is made up of one or more tasks. A task defines what action to execute, with what input.
When a task completes, it can transition into other tasks based upon criteria. Tasks can also
publish output for the next tasks. When there are no more tasks to execute, the workflow is
complete.

Orquesta includes a native language spec for the workflow definition. The language spec is
decomposed into various models and described with [JSON schema](http://json-schema.org/). A
workflow composer that understands the models converts the workflow definition into a directed
graph. The nodes represent the tasks and edges are the task transition. The criteria for task
transition is an attribute of the edge. The graph is the underpinning for conducting the workflow
execution. The workflow definition is just syntactic sugar.

Orquesta allows for one or more language specs to be defined. So as long as the workflow
definition, however structured, is composed into the expected graph, the workflow conductor can
handle it.

The workflow execution graph can be a directed graph or a directed cycle graph. It can have one or
more root nodes which are the starting tasks for the workflow. The graph can have branches that run
in parallel and then converge back to a single branch. A single branch in the graph can diverge into
multiple branches. The graph model exposes operations to identify starting tasks, get inbound and
outbound task transitions, get connected tasks, and check if cycle exists. The graph serves more
like a map for the conductor. It is stateless and does not contain any runtime data such as task
status and result.

The workflow conductor traverses the graph, directs the flow of the workflow execution, and
tracks runtime state of the execution. The conductor does not actually execute the action that is
specified for the task. The action execution is perform by another provider such as StackStorm. The
conductor directs the provider on what action to execute. As each action execution completes, the
provider relays the status and result back to the conductor. The conductor then takes the state
change, keeps track of the sequence of task execution, manages change history of the runtime
context, evaluate outbound task transitions, identifies any new tasks for execution, and determines
the overall workflow state and result.

## Copyright, License, and Contributors Agreement

Copyright 2019-2021 The StackStorm Authors.
Copyright 2014-2018 StackStorm, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this work except
in compliance with the License. You may obtain a copy of the License in the [LICENSE](LICENSE)
file or at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

By contributing you agree that these contributions are your own (or approved by your employer) and
you grant a full, complete, irrevocable copyright license to all users and developers of the
project, present and future, pursuant to the license of the project.

## Getting Help

If you need help or get stuck at any point during development, stop by on our
[Slack Community](https://stackstorm.com/community-signup) and we will do our best to assist you.
