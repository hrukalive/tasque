from tasque.models import TasqueFunctionTask, TasqueShellTask, TasqueSubprocessTask
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
                spec.args,
                spec.kwargs,
                spec.groups,
                spec.dependencies,
                spec.env,
            )
            executor.add_task(task)
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
            executor.add_task(task)
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
            executor.add_task(task)
    return executor
