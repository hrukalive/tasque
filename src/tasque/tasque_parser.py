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
        if isinstance(spec, TasqueSubprocessTask):
            task = SubprocessTask(
                tid,
                spec.name,
                spec.msg,
                spec.cwd,
                spec.cmd,
                spec.options,
                spec.groups,
                spec.dependencies,
                spec.env,
            )
            if spec.evaled_cmd:
                task.evaled_cmd = spec.evaled_cmd
            if spec.evaled_cmdline:
                task.evaled_cmdline = spec.evaled_cmdline
            if spec.evaled_options:
                task.evaled_options = spec.evaled_options
        elif isinstance(spec, TasqueShellTask):
            task = ShellTask(
                tid,
                spec.name,
                spec.msg,
                spec.cwd,
                spec.script,
                spec.shell,
                spec.groups,
                spec.dependencies,
                spec.env,
            )
            if spec.evaled_script:
                task.evaled_script = spec.evaled_script
        elif isinstance(spec, TasqueFunctionTask):
            task = FunctionTask(
                tid,
                spec.name,
                spec.msg,
                spec.func,
                name_scope[spec.func],
                spec.args,
                spec.kwargs,
                spec.groups,
                spec.dependencies,
                spec.env,
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
