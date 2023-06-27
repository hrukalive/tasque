import io
import threading
import time

from tasque.models import TasqueTaskStatus
from tasque.util import _LOG


class TasqueTask:
    def __init__(self, tid, name, msg, dependencies, groups, env):
        self.executor = None

        self.tid = tid
        self.name = name
        self.msg = msg
        self.dependencies = dependencies
        self.groups = groups
        self.env = env

        self.cancel_token = threading.Event()
        self.lock = threading.Lock()

        self.log_buf = io.StringIO()
        self.log = ''
        self.result = None
        self.status = TasqueTaskStatus.CREATED
        self.status_data = {}
        
        self.print_to_stdout = False

    def close(self):
        self.cancel()
        self.log = self.get_log()
        self.log_buf.close()

    def prepare(self, executor, print_to_stdout=True, skip_if_running=True, reset_failed_status=True):
        if self.status == TasqueTaskStatus.RUNNING and skip_if_running:
            return
        elif self.status == TasqueTaskStatus.SUCCEEDED:
            return
        elif self.status == TasqueTaskStatus.FAILED and not reset_failed_status:
            return
        else:
            self.reset()
            self.executor = executor
            self.print_to_stdout = print_to_stdout
            self.status = TasqueTaskStatus.PREPARED

    def reset(self):
        self.cancel()
        while self.status == TasqueTaskStatus.RUNNING or self.status == TasqueTaskStatus.PENDING:
            time.sleep(1)
        self.executor = None
        if self.log_buf is not None:
            self.log_buf.close()
        self.log_buf = io.StringIO()
        self.log = ''
        self.result = None
        self.status = TasqueTaskStatus.CREATED
        self.status_data = {}
        self.cancel_token.clear()

    def cancel(self):
        self.cancel_token.set()
        if self.status == TasqueTaskStatus.CREATED:
            self.status = TasqueTaskStatus.CANCELLED
            if self.executor:
                self.executor.task_cancelled(self.tid)

    def __call__(self):
        if not self.executor:
            raise Exception("Executor not set")
        if self.status != TasqueTaskStatus.PREPARED:
            _LOG(f'{self.tid} is not prepared, skip.', 'warn', self.log_buf)
            self.executor.task_skipped(self.tid)
            return self.result
        
        self.status = TasqueTaskStatus.PENDING
        while not self.executor.satisfied(self.tid, self.dependencies):
            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            time.sleep(1)
        while not self.executor.acquire_clearance_to_run(self.tid, self.groups):
            if self.cancel_token.is_set():
                self.status = TasqueTaskStatus.CANCELLED
                self.executor.task_cancelled(self.tid)
                return -1
            time.sleep(1)
        
        self.run()
    
    def run(self):
        pass
    
    def get_log(self):
        if not self.log_buf.closed:
            self.log = self.log_buf.getvalue()
        return self.log

    def get_result(self):
        return self.result
