# tasque
Pipeline executor for both functions and subprocesses tasks in Python

- Redirect stdout and stderr to task independent buffer

TODO:
- pipeline_mod_task <- function task, takes dep, results, create more tasks, and add to pipeline, and prepare, or remove tasks
  -    => save may not capture all tasks at later stage
  -    => save how to make task objects
  -    => load smarter, may need to create tasks
  -    => graph may change -> will cancellation of descendants affected?
  -    => see if task removal is possible -> possible if only created, queued, pending?
  -    => how could dependency change, depending on created tasks?
- draw graph with also the name
- pipeline modification, change dep, change param

Refactor:
- Defer cmd to before execution
- Use thread to start task directly
