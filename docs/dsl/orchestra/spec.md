# Orchestra

This DSL variation is derived from [OpenStack Mistral](https://docs.openstack.org/mistral/latest/user/dsl_v2.html).

## Why do we want to derive from Mistral?
 - The StackStorm core team is a major contributor to the design of the Mistral DSL.
 - Mistral has support for complex workflow patterns such as parallel branches, joins, for each, subworkflows, error handling, and etc.
 - Production use cases varies from simple to complex workflows. So users can progress at their pace starting with basic sequential workflows and evolve increasingly sophisticated over time.
 - The support of YAQL and Jinja expressions allow for advanced data transformation and manipulation. Some of these data related use cases are only possible with the YAQL expression language.
 - The DSL has a task model that separates task, policies, action executions, and transition.
 - Overtime, we want to converge to this new DSL and deprecate StackStorm action chains and Mistral workflows. Existing actions based on action chains and Mistral workflows will need to be ported to this new DSL. Therefore backward compatiblity needs to be considered.

## Why don't we just keep using Mistral?
- StackStorm does not have a native workflow and task models. Integration with StackStorm is awkward.
  - There are multiple handshakes between StackStorm and Mistral on execution and this result in latency and longer execution run time.
  - Mistral uses Keystone for authentication by default. Currently we don't use the Keystone auth and we do not allow external access to the Mistral API. A StackStorm auth plugin is possible but will add some UX inconvenience and add latency to the multiple handshakes between StackStorm and Mistral.
  - We will be adding a Mistral to StackStorm callback in v2.6/7 which allows Mistral to notifies StackStorm that a workflow execution is completed. We are currently using st2resultstracker to query the Mistral API. This is really inefficient and causes spike in CPU usage. Even with the callback implemented, we have to handle for network failures and callback events will be lost. The st2resulstracker for Mistral will continue to exist but the interval dialed down.
  - The display in the GUI and CLI for workflows are more or less hacks. StackStorm does not know about the task models and the children displayed are action executions. There could be tasks that does not involve any StackStormm action execution (i.e. std.noop) in which StackStorm is unaware of.
- Scaling Mistral to higher volumes of execution is difficult because it relies on the SQL database for concurrency and there're issues with deadlocks and concurrent connection limits.
- Mistral queries the database and determines the next set of tasks to run for a workflow at runtime, even when the workflow is a deterministic graph. This leads to longer transaction and multiple record locks at runtime. There is also a specific edge case where this non-graph based approach lead to miscalculation in the next set of tasks.
- To minimize deadlocks, some design decisions lead to a software architecture and code that is convoluted and takes time for developer to master. For example, to minimize the duration of the transaction, workflow engine does not execute functions immediately but are scheduled to run asynchronously and in certain case used a separate in process queue for the requests until they can be scheduled.
- Language wise, Mistral is not without its pain points.
  - The `publish` clause to define and assign variables only happens on success. In other task state, it is not possible to publish additional variables into the data context. Recently, `publish-on-error` is added to address this short coming. However, this is a patch and does not work for other cases such as pause or cancel.
  - The task model is designed to be reusable. However, the `input` clause is coupled to the action. Value of input arguments are assigned from the data context. If multiple tasks transition to this reusable task (i.e. a logging task), input arguments cannot be given directly but indirectly by publishing to the variables in the data context that will be assigned to the input arguments. Since `publish` only happens on success (and on error recently), then the input arguments for the reusable task cannot be changed if the task is not in the supported state(s).

## Design

### Workflow Model
The following is the list of attributes that make up the workflow model. A workflow takes input, perform a set of tasks in predefined order, and return output. The workflow model here is a directed graph where the tasks are the nodes and the transitions and their condition between tasks form the edges. The tasks that compose a workflow will be defined in the DSL as a dictionary named `tasks` where the key and value is the task name and task model respectively. 

| Attribute   | Description                                                       |
|-------------|-------------------------------------------------------------------|
| version     | The version of the spec being used in this workflow DSL.          |
| description | The description of the workflow.                                  |
| input       | A list of input arguments for this workflow.                      |
| vars        | A dictionary of variables defined for the scope of this workflow. |
| tasks       | A dictionary of tasks which comprise this workflow.               |
| output      | A dictionary containing the output for the workflow.              |

```yaml
version: 1.0
description: A sample workflow.
input:
    - arg1
    - arg2
vars:
    var1: 123
    var2: True
    var3: null
tasks:
    task1:
        action: core.noop
    task2:
        action: core.noop
output:
    var3: abc
    var4:
        var41: 456
        var42: def
    var5:
        - 1.0
        - 2.0
        - 3.0
```

### Task Model

| Attribute   | Description                                                       |
|-------------|-------------------------------------------------------------------|
| join        | If specified, sets up a barrier for a group of parallel branches. |
| with        | Specify the list of items to iterate through for the action.      |
| action      | The name for the action to be executed by this task.              |
| input       | A list of input arguments for the action execution.               |
| retry       | Policy to retry the action execution on given condition.          |
| on-complete | Define what happens after this task completed.                    |

As described above, the workflow is a directed graph where the tasks are the nodes and the transitions and their condition between tasks form the edges. The starting set of tasks for the workflow are tasks with no incoming edges in the graph. On completion of the task, the next set of tasks defined under `on-complete` will be invoked. Each tasks specified under `on-complete` will be represented as an outgoing edge with any `if` condition defined as an attribute of the edge. If there are no more outgoing edges identified, then the workflow execution is complete.

Each task defines what **StackStorm** action to execute, the policies around the action execution, and what happens after the task completes. All of the variables defined up to this point (aka context) is accessible to the task. At its simplest, the task invokes the action execution and pass any required input from the context to the action execution. On successful completion of the action execution, the task is evaluated for completion. If the task is completed successfully, then the next set of tasks is invoked, sequentially in the order the tasks is listed.

If more than one tasks transition to the same task and `join` is specified in the latter (i.e. the task named "task3" in the example below), then the task being transitioned to becomes a barrier for the previous parallel tasks. There will be only one instance of the barrier task. In the workflow graph, there will be multiple incoming edges for the barrier node.

Conversely, if more than one tasks transition to the same task and `join` is **not** specified in the latter (i.e. the task named "log" in the example below), then the target task will be invoked immediately following the completion of the previous task. There will be multiple instances of the target task. In the workflow graph, each invocation of the target task will be its own branch with the incoming edge from the node of the previous task.

```yaml
version: 1.0

description: Calculates (a + b) * (c + d)

input:
    - a: 0  # Defaults to value of 0 if input is not provided.
    - b: 0
    - c: 0
    - d: 0

output:
    result: <% $.abcd %>

tasks:
    task1:
        # Fully qualified name (pack.name) for the action.
        action: math.add

        # Assign input arguments to the action from the context.
        input:
            operand1: <% $.a %>
            operand2: <% $.b %>

        # Specify what to run next after the task is completed.
        on-complete:
            -   # Specify the condition that is required for this
                # task to transition to the next set of tasks.
                if: <% state() = "succeeded" %>

                # Move the publish under the transition clauses. This
                # allows for specific publish based on the task state
                # and subsequent result.
                publish:
                    msg: task1 done
                    ab: <% result() %>

                # List the tasks to run next. Each task will be
                # invoked sequentially. If more than one tasks
                # transition to the same task and a join is specified
                # at the subsequent task (i.e task1 and task2
                # transition to task3 in this case), then the
                # subsequent task becomes a barrier, with only a
                # single instance, and invocation will be blocked
                # until prior tasks are successfully completed.
                next:
                    - log
                    - task3
    task2:
        # Short hand is supported for input arguments.
        action: math.add operand1=<% $.c %> operand2=<% $.d %>
        on-complete:
            -   if: <% state() = "succeeded" %>

                # Short hand is supported for publishing variables.
                publish: msg="task2 done", cd=<% result() %>
                
                # Short hand with comma delimited list is supported.
                next: log, task3                
    task3:
        # Join is specified for this task. The invocation of this
        # task will be put on hold until the previous tasks are
        # all successfully completed.
        join: all
        action: math.multiple operand1=<% $.ab %> operand2=<% $.cd %>
        on-complete:
            -   if: <% state() = "succeeded" %>
                publish: msg="task3 done" abcd=<% result() %>
                next: log

    # Define a reusable task to log progress. Although this task is
    # referenced by multiple tasks, since there is no join defined,
    # this task is not a barrier and will be invoked separately.
    log:
        action: std.log message=<% $.msg %>
```

### Task Transition

The `on-complete` clause is a list of task transitions after a task completes. A task is completed if it either succeeded, failed, or canceled. The list of transitions will be processed in the order they are defined. In the workflow graph, each task transition is one or more outgoing edges from the current task node. For each task transition, the `if` is the condition required for the task to transition. If the condition, whether defined or not, is met, then `publish` can be defined to add new variables from the result into the context or to update existing variables in the context. Finally, the list of tasks defined in `next` will be invoked in the order they are specified.

| Attribute   | Description                                                       |
|-------------|-------------------------------------------------------------------|
| if          | An expression of the condition required for the task transition.  |
| publish     | A dictionary that define vars to be published into the context.   |
| next        | A list of tasks to be transitioned to if condition is met.        |

In the following example, if `task1` succeeds, then a message indicating success will be logged. Conversely, if `task1` fails, then a message indicating failure will be logged.

```yaml
version: '1.0'

description: Send direct message to member

input:
    - member
    - message

tasks:
    task1:
        action: slack.post
        input:
            member: <% $.member %>
            message: <% $.message %>
        on-complete:
            -   if: <% state() = "succeeded" %>
                publish: msg="Task1 succeeded."
                next: task2
            -   if: <% state() = "failed" %>
                publish: msg="Task1 failed."
                next: task2
    task2:
        action: std.log message=<% $.msg %>
```

### Special Task Transitions

The following is a list of reserved task names with special meanings. When specified under `on-complete` in `next` of each task transition, the engine will act accordingly.

| Task        | Description                                                       |
|-------------|-------------------------------------------------------------------|
| fail        | Fails the workflow execution.                                     |
| pause       | Pauses the workflow execution.                                    |
| cancel      | Cancels the workflow to indicate different state from fail.       |

```yaml
version: 1.0

description: >
    A workflow example that illustrates error handling. By default
    when any task fails, the notify_on_error task will be executed
    and the workflow will transition to the failed state.

input:
    - cmd

output:
    stdout: <% $.stdout %>

tasks:
    task1:
        action: core.local cmd=<% $.cmd %>
        on-complete:
            -   if: <% state() = "succeeded" %>
                publish: stdout=<% result().stdout %>
            -   if: <% state() = "failed" %>
                publish: stderr=<% result().stderr %>
                next: notify_on_error
    notify_on_error:
        action: core.local cmd="printf '<% $.stderr %>'"
        on-complete:
            # The fail specified here tells the workflow to go into
            # failed state on completion of the notify_on_error task.
            -   next: fail
```

### With Items

| Attribute   | Description                                                       |
|-------------|-------------------------------------------------------------------|
| items       | The list of items to iterate thru the action execution.           |
| concurrency | The number of items that can be process simultaneously.           |

The `with` model can be specified under a `task` for an `action`. A set of one of more list of items can be defined under the `with` model. The task will invoke as many action executions as there are items in parallel unless `concurrency` that specifies how may items can be processed simultaneously is specified. The result of each action execution will be appended to a list in the order of the invocation. This list of action executioin results will become the task result.

The follow is a simple example to send direct message to each member. Please note that there is a specific syntax, `item in items`, required for unwrapping the items. In this case, `member in <% $.members %>`, each item in `members` will be assigned to a variable named `member`. The `member` is then passed as input argument to each action execution.

```yaml
version: '1.0'

description: Send direct message to members

input:
    - members
    - message
    - batch_size: 3

tasks:
    task1:
        with:
            items: member in <% $.members %>
            concurrency: <% $.batch_size %>
        action: slack.post
        input:
            member: <% $.member %>
            message: <% $.message %>
```

The `items` in the `with` model can be multiple list of items. The following is an example. A list of members and messages are passed into the workflow. The workflow will zip the members and messages and process each pair individually. For this case, the syntax for each list is defined separately in a list under `items`. On each iteration, the individual `member` and `message` are processed.

```yaml
version: '1.0'

description: Send direct customized message to members

input:
    - members
    - messages
    - batch_size: 3

tasks:
    task1:
        with:
            items:
                - member in <% $.members %>
                - message in <% $.messages %>
            concurrency: <% $.batch_size %>
        action: slack.post
        input:
            member: <% $.member %>
            message: <% $.message %>
```

The following is an alternative example to processing multiple list of items. Please note the use of the zip function to process the `members` and `messages` lists and then unwrapping the items to `member` and `message`.

```yaml
version: '1.0'

description: Send direct customized message to members

input:
    - members
    - messages
    - batch_size: 3

tasks:
    task1:
        with:
            items: member, message in <% zip($.members, $.messages) %>
            concurrency: <% $.batch_size %>
        action: slack.post
        input:
            member: <% $.member %>
            message: <% $.message %>
```

### Retry Policy

| Attribute   | Description                                                       |
|-------------|-------------------------------------------------------------------|
| if          | An explicit condition to check for retry.                         |
| count       | The number of times to retry task execution.                      |
| delay       | The delay in seconds inbetween retries.                           |

This policy specifies the task to retry action execution on failure by default. The `count` defines how many times to retry the action execution and `delay` defines the interval in seconds inbetween retries.

```yaml
version: '1.0'

description: Send direct message to member

input:
    - member
    - message

tasks:
    task1:
        action: slack.post
        input:
            member: <% $.member %>
            message: <% $.message %>
        retry:
            delay: 1
            count: 3
```

An explicit condition for retry can be passed in the `if` statement. This is helpful if the action execution is always successful but returns a status code like a HTTP call. Before the retries are exhausted, the task remains incomplete. The `on-complete` clause is not evaluated until the retries are exhausted and the state of the task is determined. Therefore any published variables under `on-complete` are not available to the `retry` policy.

```yaml
version: '1.0'

description: Make a REST API call.

input:
    - url
    - headers
    - body

output:
    result: <% $.result %>

tasks:
    task1:
        action: http.post
        input:
            url: <% $.url %>
            headers: <% $.headers %>
            body: <% $.body %>
        retry:
            if: <% result().status_code not in [200, 201] %>
            delay: 1
            count: 3
        on-complete:
            -   publish:
                    result: <% result().body %> 
```
