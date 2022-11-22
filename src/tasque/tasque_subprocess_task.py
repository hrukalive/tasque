import os
import sys
import subprocess
import select
import tempfile
import time
import pathlib

from tasque.tasque_task import TasqueTask, TasqueTaskStatus
from tasque.util import _LOG

class SubprocessTask(TasqueTask):
    def __init__(self,
                 script_func,
                 root_dir,
                 tid,
                 dependencies,
                 param_args,
                 param_kwargs,
                 env_override,
                 name,
                 msg,
                 group='default'):
        super().__init__(tid, dependencies, param_args, param_kwargs, name,
                         msg, group)
        self.func = self.__cmd_task_transform(script_func, root_dir, env_override)

    @staticmethod
    def __cmd_task_transform(func, root_dir, env_override):
        def Wrapper(*args, **kwargs):
            cwd, cmd = func(*args, **kwargs)
            cmd = list(map(str, cmd))
            _LOG("Executing: {}".format(subprocess.list2cmdline(cmd)), 'info')
            proc = subprocess.Popen(cmd,
                                    cwd=str(pathlib.Path(root_dir).joinpath(cwd).resolve()),
                                    env={**os.environ, **env_override},
                                    bufsize=1,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    universal_newlines=True)
            return proc
        return Wrapper

    def __call__(self):
        if not self.executor:
            raise Exception("Executor not set")
        if self.status != TasqueTaskStatus.QUEUED:
            _LOG(f'{self.tid} is not queued, skip.', 'warn', self.output_buf)
            self.executor.task_skipped(self.tid)
            return self.result
        
        self.status = TasqueTaskStatus.PENDING
        while not self.executor.satisfied(self.tid, self.dependencies):
            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            time.sleep(1)
        while not self.executor.acquire_clearance_to_run(self.tid, self.group):
            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            time.sleep(1)

        try:
            ret_args, ret_kwargs = self._get_params()
            self.real_param_args = ret_args
            self.real_param_kwargs = ret_kwargs
            _LOG("Apply arguments: {}".format(ret_args), 'info', self.output_buf)
            _LOG("Apply keyword arguments: {}".format(ret_kwargs), 'info', self.output_buf)

            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1

            self.status = TasqueTaskStatus.RUNNING
            self.executor.task_started(self.tid)
            proc = self.func(*ret_args, **ret_kwargs)

            def read_stdout(size=-1):
                events = select.select([proc.stdout], [], [], 1)[0]
                for fd in events:
                    line = fd.read(size)
                    with self.lock:
                        self.output_buf.write(line)
                    if self.print_to_stdout:
                        sys.stdout.write(line)

            while proc.poll() is None:
                if self.cancel_token.is_set():
                    proc.kill()
                    _LOG(f"Process in {self.tid} killed", 'info', self.output_buf)
                    read_stdout()
                    self.status = TasqueTaskStatus.CANCELLED
                    self.executor.task_cancelled(self.tid)
                    return -1
                read_stdout(16)
            read_stdout()

            result = proc.wait()
            sys.stdout.flush()
        except Exception as e:
            self.status = TasqueTaskStatus.FAILED
            self.executor.task_failed(self.tid)
            _LOG(e, 'error', self.output_buf)
            return None

        self.result = result
        if result == 0:
            self.status = TasqueTaskStatus.SUCCEEDED
            self.executor.task_succeeded(self.tid)
        else:
            self.status = TasqueTaskStatus.FAILED
            self.executor.task_failed(self.tid)
        return result

def tasque_subprocess_task(tid, dependencies, name, group, msg='', param_args=[], param_kwargs={}, env_override={}):
    def Inner(func):
        def Wrapper(root_dir):
            task = SubprocessTask(func, root_dir, tid, dependencies, param_args,
                                  param_kwargs, {str(k): str(v) for k, v in env_override.items()}, name, msg, group)
            return task
        return Wrapper
    return Inner


def tasque_sh_task(tid, dependencies, name, group, msg='', param_args=[], param_kwargs={}, env_override={}):
    def Inner(func):
        def Wrapper(root_dir):
            def func_mod(*args, **kwargs):
                cwd, cmd = func(*args, **kwargs)
                script_file = tempfile.NamedTemporaryFile(mode='w+', delete=False)
                script_file.write(cmd)
                script_file.close()
                return cwd, ['sh', script_file.name]
            task = SubprocessTask(func_mod, root_dir, tid, dependencies, param_args,
                                  param_kwargs, {str(k): str(v) for k, v in env_override.items()}, name, msg, group)
            return task
        return Wrapper
    return Inner
