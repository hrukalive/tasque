from tasque.models import TasqueFunctionTask, TasqueShellTask, TasqueSubprocessTask, TasqueTaskStatus
from tasque.tasque_executor import TasqueExecutor
from tasque.tasque_function_task import FunctionTask
from tasque.tasque_sh_task import ShellTask
from tasque.tasque_subprocess_task import SubprocessTask


def tasque_parse(task_spec, communicator, name_scope={}):
    name_scope = {k: v for k, v in name_scope.items() if not k.startswith("__")}

    executor = TasqueExecutor(task_spec.name)
    executor.communicator = communicator
    executor.global_params = task_spec.global_params
    executor.global_env = task_spec.global_env
    executor.root_dir = task_spec.root_dir

    for k, v in task_spec.groups.items():
        executor.configure_group(k, v)
    for tid, spec in task_spec.tasks.items():
        assert spec.name is not None and spec.name != "", f"Empty name for task {tid}"
        if isinstance(spec, TasqueSubprocessTask):
            assert spec.cmd is not None and spec.cmd != "", f"Empty cmd for task {spec.name}"
            task = SubprocessTask(
                tid,
                spec.name,
                spec.msg or "",
                spec.cwd or ".",
                spec.cmd,
                spec.options or [],
                spec.groups or ["default"],
                spec.dependencies or [],
                spec.retry,
                spec.env or {},
            )
            if spec.evaled_cmd:
                task.evaled_cmd = spec.evaled_cmd
            if spec.evaled_cmdline:
                task.evaled_cmdline = spec.evaled_cmdline
            if spec.evaled_options:
                task.evaled_options = spec.evaled_options
        elif isinstance(spec, TasqueShellTask):
            assert spec.script is not None and spec.script != "", f"Empty script for task {spec.name}"
            task = ShellTask(
                tid,
                spec.name,
                spec.msg or "",
                spec.cwd or ".",
                spec.script,
                spec.shell or "sh",
                spec.groups or ["default"],
                spec.dependencies or [],
                spec.retry,
                spec.env or {},
            )
            if spec.evaled_script:
                task.evaled_script = spec.evaled_script
        elif isinstance(spec, TasqueFunctionTask):
            assert spec.func is not None and spec.func != "", f"Empty func for task {spec.name}"
            task = FunctionTask(
                tid,
                spec.name,
                spec.msg or "",
                spec.func,
                name_scope[spec.func],
                spec.args or [],
                spec.kwargs or [],
                spec.groups or ["default"],
                spec.dependencies or [],
                spec.retry,
                spec.env or {},
            )
            if spec.evaled_args:
                task.evaled_args = spec.evaled_args
            if spec.evaled_kwargs:
                task.evaled_kwargs = spec.evaled_kwargs
        else:
            raise ValueError(f"Unknown task type: {spec.type}")
        if spec.log:
            task.log = spec.log
        if spec.result:
            task.result = spec.result
        if spec.status:
            task.status = TasqueTaskStatus(spec.status)
        if spec.status_data:
            task.status_data = spec.status_data

        executor.add_task(task)
    return executor
