import io
import os
import pathlib
import select
import subprocess
import sys
from itertools import chain

from tasque.models import TasqueSubprocessTask, TasqueTaskStatus
from tasque.tasque_task import TasqueTask
from tasque.util import _LOG, eval_options


class SubprocessTask(TasqueTask):
    def __init__(
        self,
        tid,
        name,
        msg,
        cwd,
        cmd,
        options=[],
        groups=["default"],
        dependencies=[],
        env={},
    ):
        super().__init__(tid, name, msg, dependencies, groups, env)
        self.cwd = cwd
        self.cmd = cmd
        self.options = options
        self.evaled_cmd = None
        self.evaled_cmdline = None
        self.evaled_options = None

    def __eval_options(self):
        task_results = {tid: self.executor.get_result(tid) for tid in self.dependencies}
        eval_name_scope = {
            "task_results": task_results,
            "global_params": self.executor.global_params,
            "env": os.environ | self.executor.global_env | self.env,
            "communicator": self.executor.communicator,
            "executor": self.executor,
            "pathlib": pathlib,
        }
        self.evaled_options = eval_options(self.options, eval_name_scope)

    def reset(self):
        TasqueTask.reset(self)
        self.evaled_cmd = None
        self.evaled_cmdline = None
        self.evaled_options = None

    def run(self):
        try:
            self.__eval_options()
            _LOG("Apply options: {}".format(self.evaled_options), "info", self.log_buf)

            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            self.status = TasqueTaskStatus.RUNNING
            self.executor.task_started(self.tid)

            self.evaled_cmd = [self.cmd] + list(map(str, self.evaled_options))
            proc = subprocess.Popen(
                self.evaled_cmd,
                cwd=str(pathlib.Path(self.executor.root_dir).joinpath(self.cwd).resolve()),
                env={**os.environ, **self.executor.global_env, **self.env},
                bufsize=1,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            self.evaled_cmdline = subprocess.list2cmdline(self.evaled_cmd)
            _LOG("Executing: {}".format(self.evaled_cmdline), "info", self.log_buf)

            def read_stdout(size=-1):
                events = select.select([proc.stdout], [], [], 1)[0]
                for fd in events:
                    line = fd.read(size)
                    with self.lock:
                        self.log_buf.write(line)
                    if self.print_to_stdout:
                        sys.stdout.write(line)
            while proc.poll() is None:
                if self.cancel_token.is_set():
                    proc.kill()
                    _LOG(f"Process in {self.tid} killed", "info", self.log_buf)
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
            _LOG(e, "error", self.log_buf)
            return None
        self.result = result
        if result == 0:
            self.status = TasqueTaskStatus.SUCCEEDED
            self.executor.task_succeeded(self.tid)
        else:
            self.status = TasqueTaskStatus.FAILED
            self.executor.task_failed(self.tid)
        return result

    def state_dict(self):
        with self.lock:
            return TasqueSubprocessTask(
                name=self.name,
                msg=self.msg,
                dependencies=self.dependencies,
                groups=self.groups,
                env=self.env,
                cwd=self.cwd,
                cmd=self.cmd,
                options=self.options,
                log=self.get_log(),
                result=self.result,
                status=self.status.value,
                status_data=self.status_data,
                evaled_cmd=self.evaled_cmd,
                evaled_cmdline=self.evaled_cmdline,
                evaled_options=self.evaled_options,
            ).dict()

    def load_state_dict(self, state_dict):
        with self.lock:
            state_dict = TasqueSubprocessTask.parse_obj(state_dict)
            self.name = state_dict.name
            self.msg = state_dict.msg
            self.dependencies = state_dict.dependencies
            self.groups = state_dict.groups
            self.env = state_dict.env
            self.cwd = state_dict.cwd
            self.cmd = state_dict.cmd
            self.options = state_dict.options

            if not self.log_buf.closed:
                self.log_buf.close()
            self.log_buf = io.StringIO()
            self.log_buf.write(state_dict.log)

            self.result = state_dict.result
            self.status = TasqueTaskStatus(state_dict.status)
            self.status_data = state_dict.status_data
            self.evaled_cmd = state_dict.evaled_cmd
            self.evaled_cmdline = state_dict.evaled_cmdline
            self.evaled_options = state_dict.evaled_options
