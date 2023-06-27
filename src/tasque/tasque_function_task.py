import io
import os
import pathlib
import select
import sys
import threading
import traceback

from tasque.models import TasqueFunctionTask, TasqueTaskStatus
from tasque.std_redirector import redirect, stop_redirect
from tasque.tasque_task import TasqueTask
from tasque.util import _LOG, eval_argument


class _ThreadWithTrace(threading.Thread):
    def __init__(self, *args, **keywords):
        threading.Thread.__init__(self, *args, **keywords)
        self.killed = False
    def start(self):
        self.__run_backup = self.run
        self.run = self.__run
        threading.Thread.start(self)
    def __run(self):
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup
    def globaltrace(self, frame, event, arg):
        if event == "call":
            return self.localtrace
        else:
            return None
    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == "line":
                stop_redirect()
                raise SystemExit()
        return self.localtrace
    def kill(self):
        self.killed = True

class _FunctionTaskThread(_ThreadWithTrace):
    def __init__(self, func, param_args, param_kwargs, *args, **kwargs):
        super().__init__(target=func, args=param_args, kwargs=param_kwargs, *args, **kwargs)
        self.r_stream = None
        self.result = None
    def run(self):
        self.r_stream = redirect()
        self.exc = None
        try:
            self.result = self._target(*self._args, **self._kwargs)
        except Exception as e:
            self.exc = e
            print(traceback.format_exc())
        finally:
            stop_redirect()
    def raise_exc(self):
        if self.exc:
            raise self.exc

class FunctionTask(TasqueTask):
    def __init__(
        self,
        tid,
        name,
        msg,
        func_name,
        func,
        param_args=[],
        param_kwargs={},
        groups=["default"],
        dependencies=[],
        env={},
    ):
        super().__init__(tid, name, msg, dependencies, groups, env)
        self.func_name = func_name
        self.func = func
        self.param_args = param_args
        self.param_kwargs = param_kwargs
        self.evaled_param_args = None
        self.evaled_param_kwargs = None

    def __eval_arguments(self):
        task_results = {tid: self.executor.get_result(tid) for tid in self.dependencies}
        eval_name_scope = {
            "task_results": task_results,
            "global_params": self.executor.global_params,
            "env": os.environ | self.executor.global_env | self.env,
            "pathlib": pathlib,
        }
        self.evaled_param_args, self.evaled_param_kwargs = eval_argument(
            self.param_args, self.param_kwargs, eval_name_scope
        )

    def reset(self):
        TasqueTask.reset(self)
        self.evaled_param_args = None
        self.evaled_param_kwargs = None

    def run(self):
        try:
            self.__eval_arguments()
            _LOG("Apply arguments: {}".format(self.evaled_param_args), "info", self.log_buf)
            _LOG("Apply keyword arguments: {}".format(self.evaled_param_kwargs), "info", self.log_buf)

            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            # Run task in another thread
            self.status = TasqueTaskStatus.RUNNING
            self.executor.task_started(self.tid)

            thread = _FunctionTaskThread(
                self.func, self.evaled_param_args, self.evaled_param_kwargs
            )
            thread.start()
            while thread.r_stream is None:
                pass
            def read_stdout(size=-1):
                events = select.select([thread.r_stream], [], [], 1)[0]
                for fd in events:
                    line = fd.read(size)
                    with self.lock:
                        self.log_buf.write(line)
                    if self.print_to_stdout:
                        sys.stdout.write(line)
            while thread.is_alive():
                if self.cancel_token.is_set():
                    thread.kill()
                    _LOG(f"Thread in task {self.tid} killed", "info", self.log_buf)
                    read_stdout()
                    thread.r_stream.close()
                    self.status = TasqueTaskStatus.CANCELLED
                    self.executor.task_cancelled(self.tid)
                    return -1
                read_stdout(16)
            read_stdout()
            thread.r_stream.close()

            thread.join()
            sys.stdout.flush()
        except Exception as e:
            self.status = TasqueTaskStatus.FAILED
            self.executor.task_failed(self.tid)
            _LOG(e, "error", self.log_buf)
            return None
        try:
            thread.raise_exc()
        except Exception:
            self.status = TasqueTaskStatus.FAILED
            self.executor.task_failed(self.tid)
            return -1
        self.result = thread.result
        self.status = TasqueTaskStatus.SUCCEEDED
        self.executor.task_succeeded(self.tid)
        return thread.result

    def state_dict(self):
        with self.lock:
            ret = {
                "tid": self.tid,
                "config": TasqueFunctionTask(
                    name=self.name,
                    msg=self.msg,
                    dependencies=self.dependencies,
                    groups=self.groups,
                    env=self.env,
                    func=self.func_name,
                    args=self.param_args,
                    kwargs=self.param_kwargs
                ).dict(),
                "log": self.get_log(),
                "result": self.result,
                "status": self.status.value,
                "status_data": self.status_data,
                "evaled_param_args": self.evaled_param_args,
                "evaled_param_kwargs": self.evaled_param_kwargs,
            }
            return ret

    def load_state_dict(self, state_dict, name_scope=None):
        with self.lock:
            self.tid = state_dict["tid"]

            config = TasqueFunctionTask.parse_obj(state_dict["config"])
            self.name = config.name
            self.msg = config.msg
            self.dependencies = config.dependencies
            self.groups = config.groups
            self.env = config.env
            self.func_name = config.func
            if name_scope is not None:
                self.func = name_scope[config.func]
            self.param_args = config.args
            self.param_kwargs = config.kwargs

            if self.log_buf is not None and not self.log_buf.closed:
                self.log_buf.close()
            self.log_buf = io.StringIO()
            self.log_buf.write(state_dict["log"])

            self.result = state_dict["result"]
            self.status = TasqueTaskStatus(state_dict["status"])
            self.status_data = state_dict["status_data"]
            self.evaled_param_args = state_dict["evaled_param_args"]
            self.evaled_param_kwargs = state_dict["evaled_param_kwargs"]
