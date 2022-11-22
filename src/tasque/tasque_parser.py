from tasque.tasque_task import TasqueTaskParamKind
from tasque.tasque_function_task import tasque_function_task
from tasque.tasque_subprocess_task import tasque_subprocess_task, tasque_sh_task
from tasque.tasque_executor import TasqueExecutor

def tasque_parse(task_spec, name_scope={}):
    name_scope = {k: v for k, v in name_scope.items() if not k.startswith('__')}
    for tid, spec in task_spec['tasks'].items():
        try:
            tid = int(tid)
        except ValueError:
            raise ValueError('Task ID must be an integer')
        if 'kind' not in spec:
            raise ValueError('Task kind not specified')
        if 'param_args' in spec:
            for arg in spec['param_args']:
                if 'kind' not in arg:
                    raise ValueError('Task argument kind not specified')
                elif arg['kind'].lower() != 'extract' and arg['kind'].lower() != 'inject':
                    raise ValueError('Task argument kind must be one of "extract" or "inject"')
                elif arg['kind'].lower() == 'extract' and 'from' not in arg:
                    raise ValueError('Task argument source not specified')
                elif arg['kind'].lower() == 'inject' and 'value' not in arg:
                    raise ValueError('Task argument value not specified')
        if 'param_kwargs' in spec:
            for arg in spec['param_kwargs'].values():
                if 'kind' not in arg:
                    raise ValueError('Task argument kind not specified')
                elif arg['kind'].lower() != 'extract' and arg['kind'].lower() != 'inject':
                    raise ValueError('Task argument kind must be one of "extract" or "inject"')
                elif arg['kind'].lower() == 'extract' and 'from' not in arg:
                    raise ValueError('Task argument source not specified')
                elif arg['kind'].lower() == 'inject' and 'value' not in arg:
                    raise ValueError('Task argument value not specified')
        if spec['kind'].lower() == 'function':
            if 'func_signature' not in spec:
                raise ValueError('Function task function signature not specified')
            if 'dependencies' not in spec:
                raise ValueError('Function task dependencies not specified')
            if 'name' not in spec:
                raise ValueError('Function task name not specified')
            if spec['func_signature'] not in name_scope:
                raise ValueError('Function task function signature not found in local scope')
        elif spec['kind'].lower() == 'subprocess':
            if 'dependencies' not in spec:
                raise ValueError('Subprocess task dependencies not specified')
            if 'name' not in spec:
                raise ValueError('Subprocess task name not specified')
            if 'cwd' not in spec:
                raise ValueError('Subprocess task working directory not specified')
            if 'cmd' not in spec:
                raise ValueError('Subprocess task command not specified')
        elif spec['kind'].lower() == 'sh':
            if 'dependencies' not in spec:
                raise ValueError('Shell task dependencies not specified')
            if 'name' not in spec:
                raise ValueError('Shell task name not specified')
            if 'cwd' not in spec:
                raise ValueError('Shell task working directory not specified')
            if 'cmd' not in spec:
                raise ValueError('Shell task script not specified')
        else:
            raise ValueError('Task kind not recognized')

    executor = TasqueExecutor()
    executor.set_task_spec(task_spec)
    executor.set_global_param(task_spec.get('global_params', {}))
    executor.set_root_dir(task_spec.get('root_dir', '.'))
    for k, v in task_spec.get('groups', {}).items():
        executor.configure_group(k, v)
    
    for tid, spec in task_spec.get('tasks', {}).items():
        tid = int(tid)
        dependencies = spec['dependencies']
        name = spec['name']
        msg = spec.get('msg', '')
        group = spec.get('group', 'default')
        param_args = []
        for arg in spec.get('param_args', []):
            if arg['kind'].lower() == 'inject':
                param_args.append((TasqueTaskParamKind.INJECT, arg['value']))
            elif arg['kind'].lower() == 'extract':
                param_args.append((TasqueTaskParamKind.EXTRACT, (arg['from'], arg['position']) if 'position' in arg else arg['from']))
        param_kwargs = {}
        for k, arg in spec.get('param_kwargs', {}).items():
            if arg['kind'].lower() == 'inject':
                param_kwargs[k] = (TasqueTaskParamKind.INJECT, arg['value'])
            elif arg['kind'].lower() == 'extract':
                param_kwargs[k] = (TasqueTaskParamKind.EXTRACT, (arg['from'], arg['position']) if 'position' in arg else arg['from'])
        env_override = spec.get('env_override', {})

        if spec['kind'].lower() == 'function':
            func = name_scope[spec['func_signature']]
            task_ret = tasque_function_task(tid=tid, dependencies=dependencies, name=name, msg=msg, group=group, param_args=param_args, param_kwargs=param_kwargs)(func)
            executor.add_task(task_ret)
        elif spec['kind'].lower() == 'subprocess':
            def wrapper():
                cwd = spec['cwd']
                cmd = spec['cmd']
                @tasque_subprocess_task(tid=tid, dependencies=dependencies, name=name, msg=msg, group=group, param_args=param_args, param_kwargs=param_kwargs, env_override=env_override)
                def task_ret(*args, **kwargs):
                    return cwd, [eval(f'f"""{template}"""', name_scope | {'args': args, 'kwargs': kwargs}) for template in cmd]
                return task_ret
            executor.add_task(wrapper())
        elif spec['kind'].lower() == 'sh':
            def wrapper():
                cwd = spec['cwd']
                cmd = spec['cmd']
                @tasque_sh_task(tid=tid, dependencies=dependencies, name=name, msg=msg, group=group, param_args=param_args, param_kwargs=param_kwargs, env_override=env_override)
                def task_ret(*args, **kwargs):
                    return cwd, eval(f'f"""{cmd}"""', name_scope | {'args': args, 'kwargs': kwargs})
                return task_ret
            executor.add_task(wrapper())
        else:
            raise ValueError('Invalid task type: {}'.format(task_spec['type']))
    return executor
