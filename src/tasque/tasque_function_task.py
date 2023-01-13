import sys
import select
import threading
import time
import traceback

from tasque.tasque_task import TasqueTask, TasqueTaskStatus
from tasque.std_redirector import redirect, stop_redirect
from tasque.util import _LOG

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
        if event == 'call':
            return self.localtrace
        else:
            return None

    def localtrace(self, frame, event, arg):
        if self.killed:
            if event == 'line':
                stop_redirect()
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True

class _FunctionTaskThread(_ThreadWithTrace):
    def __init__(self, func, param_args, param_kwargs, *args, **kwargs):
        super().__init__(target=func,
                         args=param_args,
                         kwargs=param_kwargs,
                         *args,
                         **kwargs)
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
    def __init__(self,
                 func,
                 tid,
                 dependencies,
                 param_args,
                 param_kwargs,
                 name,
                 msg,
                 group='default'):
        super().__init__(tid, dependencies, param_args, param_kwargs, name,
                         msg, group)
        self.func = func

    def __call__(self):
        if not self.executor:
            raise Exception("Executor not set")
        if self.status != TasqueTaskStatus.PREPARED:
            _LOG(f'{self.tid} is not prepared, skip.', 'warn', self.output_buf)
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

            # Run task in another thread
            self.status = TasqueTaskStatus.RUNNING
            self.executor.task_started(self.tid)
            thread = _FunctionTaskThread(self.func, ret_args, ret_kwargs)
            thread.start()
            while thread.r_stream is None:
                pass

            def read_stdout(size=-1):
                events = select.select([thread.r_stream], [], [], 1)[0]
                for fd in events:
                    line = fd.read(size)
                    with self.lock:
                        self.output_buf.write(line)
                    if self.print_to_stdout:
                        sys.stdout.write(line)

            while thread.is_alive():
                if self.cancel_token.is_set():
                    thread.kill()
                    _LOG(f"Thread in task {self.tid} killed", 'info', self.output_buf)
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
            _LOG(e, 'error', self.output_buf)
            return None

        try:
            thread.raise_exc()
        except Exception as e:
            self.status = TasqueTaskStatus.FAILED
            self.executor.task_failed(self.tid)
            return -1

        self.result = thread.result
        self.status = TasqueTaskStatus.SUCCEEDED
        self.executor.task_succeeded(self.tid)
        return thread.result


def tasque_function_task(tid, dependencies, name, group, msg='', param_args=[], param_kwargs={}):
    def Inner(func):
        def Wrapper(*args, **kwargs):
            task = FunctionTask(func, tid, dependencies, param_args,
                                param_kwargs, name, msg, group)
            return task
        return Wrapper
    return Inner
