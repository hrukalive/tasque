import concurrent.futures
import threading
import time

import networkx as nx

# import matplotlib.pyplot as plt
from tasque.communicator import TasqueCommunicator
from tasque.models import (
    TasqueExternalAcquisition,
    TasqueExternalStateDependency,
    TasqueSpecification,
    TasqueTaskStatus,
)
from tasque.util import _LOG, set_logger


class TasqueExecutor(object):
    def __init__(self, eid) -> None:
        self.eid = eid
        self.communicator = TasqueCommunicator()
        self.tasks = {}
        self.groups = {'default': {'acquired': set(), 'capacity': -1}}
        self.global_lock = threading.Lock()
        self.executor: concurrent.futures.Executor = None
        self.futures = {}
        self.tid_graph: nx.DiGraph = None
        self.global_params = {}
        self.global_env = {}
        self.root_dir = '/'

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

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

    def add_task(self, task):
        if task.tid == 0:
            raise Exception("Task ID 0 is reserved for global parameters")
        if task.tid in self.tasks:
            raise Exception("Task with id {} already exists".format(task.tid))
        self.tasks[task.tid] = task

    def pre_check(self):
        g = []
        for task in self.tasks.values():
            for t_g in task.groups:
                if isinstance(t_g, str) and t_g not in self.groups:
                    raise Exception("Task group {} not configured".format(
                        task.group))
            for dep in task.dependencies:
                if isinstance(dep, int) and dep not in self.tasks:
                    raise Exception("Task {} not found".format(dep))
                g.append((dep, task.tid))
        self.tid_graph = nx.DiGraph(g)
        for task in self.tasks.values():
            self.tid_graph.add_node(task.tid)
        try:
            nx.find_cycle(self.tid_graph, orientation='original')
            raise Exception("Cycle detected in task graph")
        except nx.exception.NetworkXNoCycle:
            pass

    def prepare(self, print_to_stdout=True, skip_if_running=True, reset_failed_status=True):
        self.pre_check()

        for task in self.tasks.values():
            task.prepare(self, print_to_stdout, skip_if_running, reset_failed_status)

        if self.executor is None:
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=len(self.tasks) * 2, thread_name_prefix="tasque")
        return self.tid_graph

    def satisfied(self, tid, dependencies):
        for dependency in dependencies:
            if isinstance(dependency, TasqueExternalStateDependency):
                if not self.communicator.satisfied(self.eid, tid, dependency):
                    return False
            elif self.tasks[dependency].status != TasqueTaskStatus.SUCCEEDED:
                return False
        return True

    # If all dependencies are satisfied, then this task is ready to run.
    # Check for clearance to run in the group.
    def acquire_clearance_to_run(self, tid, groups):
        with self.global_lock and self.communicator.lock:
            copied_groups = self.groups.copy()
            group_names, external = [], []
            for g in groups:
                if isinstance(g, TasqueExternalAcquisition):
                    external.append(g)
                elif isinstance(g, str):
                    group_names.append(g)
                    
            local_satisfied = True
            for group_name in group_names:
                acquired = copied_groups[group_name]['acquired']
                capacity = copied_groups[group_name]['capacity']
                if tid in acquired:
                    continue
                elif capacity == -1 or len(acquired) < capacity:
                    acquired.add(tid)
                else:
                    local_satisfied = False
                    break
                
            if local_satisfied and self.communicator.acquire_clearance_to_run(self.eid, tid, external):
                self.groups = copied_groups
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

    def get_log(self, tid):
        return self.tasks[tid].get_log()

    def get_result(self, tid):
        return self.tasks[tid].get_result()

    def get_status(self, tid):
        return self.tasks[tid].status, self.tasks[tid].status_data

    def task_started(self, tid):
        group_names, external = [], []
        for g in self.tasks[tid].groups:
            if isinstance(g, TasqueExternalAcquisition):
                external.append(g)
            elif isinstance(g, str):
                group_names.append(g)
        for gn in group_names:
            if tid not in self.groups[gn]['acquired']:
                raise Exception("Task {} not in the clear to run.".format(tid))
        if not self.communicator.is_clear_to_run(self.eid, tid, external):
            raise Exception("Task {} not in the clear to run.".format(tid))

        self.tasks[tid].status_data['start_time'] = time.time()
        _LOG("Task {} started".format(tid), 'info')

    def __task_end(self, tid):
        with self.global_lock and self.communicator.lock:
            copied_groups = self.groups.copy()
            group_names, external = [], []
            for g in self.tasks[tid].groups:
                if isinstance(g, TasqueExternalAcquisition):
                    external.append(g)
                elif isinstance(g, str):
                    group_names.append(g)

            for group_name in group_names:
                copied_groups[group_name]['acquired'].discard(tid)
            if self.communicator.release_acquired(self.eid, tid, external):
                self.groups = copied_groups

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
    
    def is_failed(self):
        for task in self.tasks.values():
            if task.status == TasqueTaskStatus.FAILED:
                return True
    
    def is_cancelled(self):
        for task in self.tasks.values():
            if task.status == TasqueTaskStatus.CANCELLED:
                return True

    def state_dict(self):
        return TasqueSpecification(
            name=self.eid,
            global_params=self.global_params,
            global_env=self.global_env,
            root_dir=self.root_dir,
            groups={k: v['capacity'] for k, v in self.groups.items() if k != 'default'},
            tasks={task.tid: task.state_dict() for task in self.tasks.values()}
        ).dict()

    def load_state_dict(self, state_dict, load_name, load_global_params, load_global_env, load_root_dir, load_groups, load_tasks, name_scope=None):
        state_dict = TasqueSpecification.parse_obj(state_dict)
        if load_name:
            self.eid = state_dict.name
        if load_global_params:
            self.global_params = state_dict.global_params
        if load_global_env:
            self.global_env = state_dict.global_env
        if load_root_dir:
            self.root_dir = state_dict.root_dir
        if load_groups:
            for k, v in state_dict.groups.items():
                self.configure_group(k, v)
        if load_tasks:
            for task in self.tasks.values():
                if task.tid in state_dict.tasks:
                    task.load_state_dict(state_dict.tasks[task.tid], name_scope)
