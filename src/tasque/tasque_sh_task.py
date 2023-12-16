import io
import os
import pathlib
import select
import subprocess
import sys
import tempfile

from tasque.models import TasqueRetry, TasqueShellTask, TasqueTaskStatus
from tasque.tasque_task import TasqueTask
from tasque.util import _LOG


class ShellTask(TasqueTask):
    def __init__(
        self,
        tid,
        name,
        msg,
        cwd,
        script,
        shell="sh",
        groups=["default"],
        dependencies=[],
        retry=TasqueRetry(),
        env={}
    ):
        super().__init__(
            tid=tid,
            name=name,
            msg=msg,
            groups=groups,
            dependencies=dependencies,
            retry=retry,
            env=env
        )
        self.cwd = cwd
        self.shell = shell
        self.script = script
        self.evaled_script = None

    def __eval_script(self):
        task_results = {tid: self.executor.get_result(tid) for tid in self.dependencies}
        eval_name_scope = {
            "task_results": task_results,
            "global_params": self.executor.global_params,
            "env": os.environ | self.executor.global_env | self.env,
            "communicator": self.executor.communicator,
            "executor": self.executor,
            "pathlib": pathlib,
            "TasqueTaskStatus": TasqueTaskStatus,
        }
        _LOG("Evaluating script ...", "info", self.log_buf)
        self.evaled_script = eval(f'f"""{self.script}"""', eval_name_scope)
        _LOG("Evaluated script", "info", self.log_buf)

    def reset(self):
        TasqueTask.reset(self)
        self.evaled_script = None

    def run(self):
        try:
            self.__eval_script()
            _LOG("Script: {}".format(self.evaled_script), "info", self.log_buf)

            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            self.status = TasqueTaskStatus.RUNNING
            self.executor.task_started(self.tid)

            script_file = tempfile.NamedTemporaryFile(mode="w+", delete=False)
            script_file.write(self.evaled_script)
            script_file.close()
            cmd = [self.shell, script_file.name]
            proc = subprocess.Popen(
                cmd,
                cwd=str(pathlib.Path(self.executor.root_dir).joinpath(self.cwd).resolve()),
                env={**os.environ, **self.executor.global_env, **self.env},
                bufsize=1,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
            )
            self.cmd_line = subprocess.list2cmdline(cmd)
            _LOG("Executing: {}".format(self.cmd_line), "info", self.log_buf)

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
        else:
            self.result = result
            if result == 0:
                self.status = TasqueTaskStatus.SUCCEEDED
                self.executor.task_succeeded(self.tid)
            else:
                self.status = TasqueTaskStatus.FAILED
                self.executor.task_failed(self.tid)
            return result
        return None

    def state_dict(self):
        with self.lock:
            return TasqueShellTask(
                name=self.name,
                msg=self.msg,
                groups=self.groups,
                dependencies=self.dependencies,
                retry=self.retry,
                env=self.env,
                cwd=self.cwd,
                shell=self.shell,
                script=self.script,
                log=self.get_log(),
                result=self.result,
                status=self.status.value,
                status_data=self.status_data,
                evaled_script=self.evaled_script,
            ).dict()

    def load_state_dict(self, state_dict, name_scope=None):
        with self.lock:
            state_dict = TasqueShellTask.parse_obj(state_dict)
            self.name = state_dict.name
            self.msg = state_dict.msg
            self.groups = state_dict.groups
            self.dependencies = state_dict.dependencies
            self.retry = state_dict.retry
            self.env = state_dict.env
            self.cwd = state_dict.cwd
            self.shell = state_dict.shell
            self.script = state_dict.script

            if not self.log_buf.closed:
                self.log_buf.close()
            self.log_buf = io.StringIO()
            self.log_buf.write(state_dict.log)

            self.result = state_dict.result
            self.status = TasqueTaskStatus(state_dict.status)
            self.status_data = state_dict.status_data
            self.evaled_script = state_dict.evaled_script
