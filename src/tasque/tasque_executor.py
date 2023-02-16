import threading
import concurrent.futures
import networkx as nx
import time
# import matplotlib.pyplot as plt

from tasque.tasque_task import TasqueTaskStatus, TasqueTaskParamKind
from tasque.util import _LOG, set_logger

class TasqueExecutor(object):
    def __init__(self):
        self.tasks = {}
        self.groups = {'default': {'acquired': set(), 'capacity': -1}}
        self.global_lock = threading.Lock()
        self.executor = None
        self.futures = {}
        self.tid_graph = None
        self.global_params = {}
        self.global_env_override = {}
        self.root_dir = '/'
        self.task_spec = {}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def set_global_params(self, mapping):
        self.global_params = mapping
        self.task_spec['global_params'] |= mapping
    def set_global_env_override(self, mapping):
        self.global_env_override = mapping
        self.task_spec['global_env_override'] |= mapping
    def set_root_dir(self, root_dir):
        self.root_dir = root_dir
        self.task_spec['root_dir'] = root_dir
    def set_task_spec(self, task_spec):
        self.task_spec = task_spec
    def set_logger(self, logger):
        set_logger(logger)

    def close(self):
        self.cancel()
        if self.executor:
            self.executor.shutdown(wait=True)
            self.executor = None
        for task in self.tasks.values():
            task.close()

    def configure_group(self, name, capacity):
        self.groups[name] = {'acquired': set(), 'capacity': capacity}
        self.task_spec['groups'][name] = capacity

    def add_task(self, task):
        task = task(self.root_dir)
        if task.tid == 0:
            raise Exception("Task ID 0 is reserved for global parameters")
        if task.tid in self.tasks:
            raise Exception("Task with id {} already exists".format(task.tid))
        self.tasks[task.tid] = task

    def pre_check(self):
        g = []
        for task in self.tasks.values():
            if task.group not in self.groups:
                raise Exception("Task group {} not configured".format(
                    task.group))
            for dep in task.dependencies:
                if dep not in self.tasks:
                    raise Exception("Task {} not found".format(dep))
                g.append((dep, task.tid))
        tid_graph = nx.DiGraph(g)
        for task in self.tasks.values():
            tid_graph.add_node(task.tid)
        try:
            nx.find_cycle(tid_graph, orientation='original')
            raise Exception("Cycle detected in task graph")
        except nx.exception.NetworkXNoCycle:
            pass
        for task in self.tasks.values():
            for (kind, param) in task.param_args + list(task.param_kwargs.values()):
                if kind == TasqueTaskParamKind.EXTRACT:
                    if isinstance(param, int):
                        dep = param
                    elif isinstance(param, tuple):
                        dep, _ = param
                    else:
                        raise Exception("Invalid param specification")
                    if dep == 0:
                        continue
                    if not nx.algorithms.has_path(tid_graph, dep, task.tid):
                        raise Exception(
                            "Task {} need to extract parameter from task {}, which must be its dependency.".format(task.tid, dep))
        return tid_graph

    def prepare(self, print_to_stdout=True, skip_if_running=True, reset_failed_status=True):
        self.tid_graph = self.pre_check()

        for task in self.tasks.values():
            task.prepare(self, print_to_stdout, skip_if_running, reset_failed_status)

        if self.executor is None:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.tasks) * 2, thread_name_prefix="tasque")
        return self.tid_graph

    def satisfied(self, tid, dependencies):
        for dependency in dependencies:
            if self.tasks[dependency].status != TasqueTaskStatus.SUCCEEDED:
                return False
        return True

    # If all dependencies are satisfied, then this task is ready to run.
    # Check for clearance to run in the group.
    def acquire_clearance_to_run(self, tid, group_name):
        with self.global_lock:
            acquired = self.groups[group_name]['acquired']
            capacity = self.groups[group_name]['capacity']
            if tid in acquired:
                return True
            elif capacity == -1 or len(acquired) < capacity:
                acquired.add(tid)
                return True
        return False

    def cancel(self, tid=None):
        if not tid:
            for task in self.tasks.values():
                task.cancel()
        else:
            self.tasks[tid].cancel()
            for d_tid in nx.descendants(self.tid_graph, tid):
                self.tasks[d_tid].cancel()

    def reset(self, tid=None):
        if not tid:
            self.cancel()
            for task in self.tasks.values():
                task.reset()
        else:
            self.tasks[tid].reset()
            for d_tid in nx.descendants(self.tid_graph, tid):
                self.tasks[d_tid].reset()

    def get_all_tids(self):
        return list(self.tasks.keys())

    def get_output(self, tid):
        return self.tasks[tid].get_output()

    def get_result(self, tid):
        return self.tasks[tid].get_result()

    def get_status(self, tid):
        return self.tasks[tid].status, self.tasks[tid].status_data

    def task_started(self, tid):
        if tid not in self.groups[self.tasks[tid].group]['acquired']:
            raise Exception("Task {} not in the clear to run.".format(tid))
        else:
            self.tasks[tid].status_data['start_time'] = time.time()
        _LOG("Task {} started".format(tid), 'info')

    def __task_end(self, tid):
        with self.global_lock:
            self.groups[self.tasks[tid].group]['acquired'].discard(tid)
        if 'start_time' in self.tasks[tid].status_data:
            end_time = time.time()
            self.tasks[tid].status_data['end_time'] = end_time
            self.tasks[tid].status_data['time_elapsed'] = end_time - self.tasks[tid].status_data['start_time']

    def task_succeeded(self, tid):
        self.__task_end(tid)
        _LOG("Task {} succeeded".format(tid), 'info')

    def task_failed(self, tid):
        self.__task_end(tid)
        _LOG("Task {} failed".format(tid), 'warn')
        self.cancel(tid)

    def task_skipped(self, tid):
        self.__task_end(tid)
        _LOG("Task {} skipped".format(tid), 'info')
        self.cancel(tid)

    def task_cancelled(self, tid):
        self.__task_end(tid)
        _LOG("Task {} cancelled".format(tid), 'info')

    def execute(self):
        for task in self.tasks.values():
            if task.status == TasqueTaskStatus.FAILED:
                self.cancel(task.tid)
        for task in self.tasks.values():
            if task.status == TasqueTaskStatus.PREPARED:
                if task.tid in self.futures:
                    try:
                        self.futures[task.tid].result(5)
                    except concurrent.futures.TimeoutError:
                        self.futures[task.tid].cancel()
                        self.futures.pop(task.tid)
                self.futures[task.tid] = self.executor.submit(task)

    def wait(self):
        for future in concurrent.futures.as_completed(self.futures.values()):
            future.result()

    def is_running(self):
        for task in self.tasks.values():
            if task.status == TasqueTaskStatus.RUNNING or task.status == TasqueTaskStatus.PENDING:
                return True

    def is_successful(self):
        for task in self.tasks.values():
            if task.status != TasqueTaskStatus.SUCCEEDED:
                return False
        return True

    def get_save(self, with_output=False):
        save = {
            'global_params': self.global_params,
            'global_env_override': self.global_env_override,
            'root_dir': self.root_dir,
            'task_spec': self.task_spec,
            'groups': {k: v['capacity'] for k, v in self.groups.items() if k != 'default'},
            'tasks': {}
        }
        for task in self.tasks.values():
            save['tasks'][task.tid] = task.get_save(with_output)
        return save

    def restore_save(self, save, load_global_params, load_global_env_override, load_root_dir, load_groups, load_task_spec, load_tasks):
        if load_global_params:
            self.global_params = save.get('global_params', {})
        if load_global_env_override:
            self.global_env_override = save.get('global_env_override', {})
        if load_root_dir:
            self.root_dir = save.get('root_dir', '.')
        if load_task_spec:
            self.task_spec = save.get('task_spec', {})
        if load_groups:
            for k, v in save.get('groups', {}).items():
                self.configure_group(k, v)
        if load_tasks:
            for task in self.tasks.values():
                if task.tid in save.get('tasks', {}):
                    task.restore_save(save['tasks'][task.tid])
