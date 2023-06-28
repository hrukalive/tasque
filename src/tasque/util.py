import logging

from tasque.models import (
    TasqueTaskArgument,
    TasqueTaskDictArgument,
    TasqueTaskFstrArgument,
    TasqueTaskListArgument,
)

LOGGER = None

def set_logger(logger):
    global LOGGER
    LOGGER = logger

def set_default_logger():
    global LOGGER
    LOGGER = logging.getLogger("pipeline_executor")

def _LOG(msg, level, buf=None):
    msg = str(msg)
    if LOGGER:
        if level == "info":
            LOGGER.info(msg)
        elif level == "debug":
            LOGGER.debug(msg)
        elif level == "warn":
            LOGGER.warning(msg)
        elif level == "error":
            LOGGER.error(msg)
        else:
            LOGGER.info(msg)
    if buf is not None:
        buf.write(f"[{level}] {msg}\n")

def eval_arguments(param_args, param_kwargs, eval_name_scope):
    evaled_param_args = []
    for arg in param_args:
        if isinstance(arg, str):
            evaled_param_args.append(arg)
        elif isinstance(arg, TasqueTaskFstrArgument) and eval(arg.cond, eval_name_scope):
            evaled_param_args.append(eval(f'f"""{arg.expr}"""', eval_name_scope))
        elif isinstance(arg, TasqueTaskListArgument) and eval(arg.cond, eval_name_scope):
            ret = eval(arg.expr, eval_name_scope)
            assert isinstance(ret, list)
            evaled_param_args.extend(ret)
        elif isinstance(arg, TasqueTaskArgument) and eval(arg.cond, eval_name_scope):
            evaled_param_args.append(eval(arg.expr, eval_name_scope))

    evaled_param_kwargs = {}
    for arg in param_kwargs:
        if isinstance(arg, TasqueTaskDictArgument) and eval(arg.cond, eval_name_scope):
            tmp = eval(arg.expr, eval_name_scope)
            assert isinstance(tmp, dict)
            evaled_param_kwargs.update(tmp)
        elif isinstance(arg, dict):
            for key, subarg in arg.items():
                if isinstance(subarg, str):
                    evaled_param_kwargs[key] = subarg
                elif isinstance(subarg, TasqueTaskFstrArgument) and eval(subarg.cond, eval_name_scope):
                    evaled_param_kwargs[key] = eval(f'f"""{subarg.expr}"""', eval_name_scope)
                elif isinstance(subarg, TasqueTaskArgument) and eval(subarg.cond, eval_name_scope):
                    evaled_param_kwargs[key] = eval(subarg.expr, eval_name_scope)
    return evaled_param_args, evaled_param_kwargs

def eval_options(param_options, eval_name_scope):
    evaled_options = []
    for option in param_options:
        if isinstance(option, str):
            evaled_options.append(option)
        elif isinstance(option, dict):
            for k, subarg in option.items():
                if isinstance(subarg, str):
                    evaled_options.append(k)
                    evaled_options.append(subarg)
                elif isinstance(subarg, TasqueTaskFstrArgument) and eval(subarg.cond, eval_name_scope):
                    evaled_options.append(k)
                    evaled_options.append(eval(f'f"""{subarg.expr}"""', eval_name_scope))
                elif isinstance(subarg, TasqueTaskListArgument) and eval(subarg.cond, eval_name_scope):
                    tmp = eval(subarg.expr, eval_name_scope)
                    assert isinstance(tmp, list)
                    evaled_options.append(k)
                    evaled_options.extend(tmp)
                elif isinstance(subarg, TasqueTaskArgument) and eval(subarg.cond, eval_name_scope):
                    evaled_options.append(k)
                    evaled_options.append(eval(subarg.expr, eval_name_scope))
        elif isinstance(option, TasqueTaskDictArgument) and eval(option.cond, eval_name_scope):
            tmp = eval(option.expr, eval_name_scope)
            assert isinstance(tmp, dict)
            for k, v in tmp.items():
                evaled_options.append(k)
                evaled_options.append(v)
        elif isinstance(option, TasqueTaskListArgument) and eval(option.cond, eval_name_scope):
            tmp = eval(option.expr, eval_name_scope)
            assert isinstance(tmp, list)
            evaled_options.extend(tmp)
        elif isinstance(option, TasqueTaskFstrArgument) and eval(option.cond, eval_name_scope):
            evaled_options.append(eval(f'f"""{option.expr}"""', eval_name_scope))
        elif isinstance(option, TasqueTaskArgument) and eval(option.cond, eval_name_scope):
            evaled_options.append(eval(option.expr, eval_name_scope))
    return evaled_options
