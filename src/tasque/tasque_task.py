from enum import Enum
import threading
import io
import time

from tasque.util import _LOG

class TasqueTaskStatus(int, Enum):
    QUEUED = 0
    PENDING = 1
    RUNNING = 2
    SUCCEEDED = 3
    FAILED = 4
    CANCELLED = 5

class TasqueTaskParamKind(int, Enum):
    EXTRACT = 1
    INJECT = 2

class TasqueTask:
    def __init__(self, tid, dependencies, param_args, param_kwargs, name, msg,
                 group):
        self.executor = None

        self.tid = tid
        self.dependencies = dependencies
        self.param_args = param_args
        self.param_kwargs = param_kwargs
        self.name = name
        self.msg = msg
        self.group = group

        self.cancel_token = threading.Event()
        self.lock = threading.Lock()

        self.output_buf = io.StringIO()
        self.output = ''
        self.result = None
        self.status = None
        self.status_data = {}

        self.real_param_args = None
        self.real_param_kwargs = None
        
        self.print_to_stdout = False

    def _get_params(self):
        ret_args = []
        ret_kwargs = {}
        # List of Union[int, tuple[int, int], tuple[int, str]]
        for (kind, param) in self.param_args:
            if kind == TasqueTaskParamKind.EXTRACT:
                if isinstance(param, int):
                    dep = param
                    pos = None
                elif isinstance(param, tuple):
                    dep, pos = param
                else:
                    raise Exception("Invalid param specification")
                ret = self.executor.get_result(dep) if dep > 0 else self.executor.global_params
                if pos is None:
                    ret_args.append(ret)
                elif isinstance(pos, int) or isinstance(pos, str):
                    ret_args.append(ret[pos])
                else:
                    raise Exception("Invalid position for parameter")
            elif kind == TasqueTaskParamKind.INJECT:
                ret_args.append(param)
            else:
                raise Exception("Invalid param specification")
        for kw, (kind, param) in self.param_kwargs.items():
            if kind == TasqueTaskParamKind.EXTRACT:
                if isinstance(param, int):
                    dep = param
                    pos = None
                elif isinstance(param, tuple):
                    dep, pos = param
                else:
                    raise Exception("Invalid param specification")
                ret = self.executor.get_result(dep) if dep > 0 else self.executor.global_params
                if pos is None:
                    ret_kwargs[kw] = ret
                elif isinstance(pos, int) or isinstance(pos, str):
                    ret_kwargs[kw] = ret[pos]
                else:
                    raise Exception("Invalid position for parameter")
            elif kind == TasqueTaskParamKind.INJECT:
                ret_kwargs[kw] = param
            else:
                raise Exception("Invalid param specification")
        return ret_args, ret_kwargs

    def close(self):
        self.cancel()
        self.output = self.get_output()
        self.output_buf.close()

    def prepare(self, skip_if_running=True, reset_failed_status=True):
        if self.status == TasqueTaskStatus.RUNNING and skip_if_running:
            return
        elif self.status == TasqueTaskStatus.SUCCEEDED:
            return
        elif self.status == TasqueTaskStatus.FAILED and not reset_failed_status:
            return
        else:
            self.reset()
            self.status = TasqueTaskStatus.QUEUED

    def reset(self):
        if self.status == TasqueTaskStatus.RUNNING:
            self.cancel()
        while self.status == TasqueTaskStatus.RUNNING:
            time.sleep(1)
        self.result = None
        self.real_param_args = None
        self.real_param_kwargs = None
        self.status = None
        self.status_data = {}
        if self.output_buf is not None:
            self.output_buf.close()
        self.output_buf = io.StringIO()
        self.cancel_token.clear()

    def cancel(self):
        if self.status == TasqueTaskStatus.RUNNING:
            self.cancel_token.set()
        elif self.status != TasqueTaskStatus.SUCCEEDED and self.status != TasqueTaskStatus.FAILED and self.status != TasqueTaskStatus.CANCELLED:
            self.cancel_token.set()
            self.status = TasqueTaskStatus.CANCELLED
            self.executor.task_cancelled(self.tid)

    def get_output(self):
        with self.lock:
            if not self.output_buf.closed:
                self.output = self.output_buf.getvalue()
        return self.output

    def get_result(self):
        return self.result

    def get_save(self, with_output=False):
        ret = {
            "tid": self.tid,
            "dependencies": self.dependencies,
            "real_param_args": self.real_param_args,
            "real_param_kwargs": self.real_param_kwargs,
            "name": self.name,
            "msg": self.msg,
            "group": self.group,
            "status": self.status,
            "status_data": self.status_data,
            "result": self.result,
        }
        if with_output:
            ret["output"] = self.get_output()
        return ret

    def restore_save(self, save):
        # self.tid = save["tid"]
        # self.dependencies = save["dependencies"]
        # self.param_args = save["param_args"]
        # self.param_kwargs = save["param_kwargs"]
        # self.name = save["name"]
        # self.msg = save["msg"]
        # self.group = save["group"]
        self.status = save.get("status", None)
        self.status_data = save.get("status_data", {})
        self.result = save.get("result", None)
        if 'output' in save:
            if self.output_buf is not None:
                self.output_buf.close()
            self.output_buf = io.StringIO(save["output"])
