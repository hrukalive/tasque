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

def eval_argument(param_args, param_kwargs, eval_name_scope):
    evaled_param_args = []
    for arg in param_args:
        if isinstance(arg, str):
            evaled_param_args.append(arg)
        elif eval(arg.cond, eval_name_scope):
            if isinstance(arg, TasqueTaskFstrArgument):
                evaled_param_args.append(eval(f'f"""{arg.expr}"""', eval_name_scope))
            elif isinstance(arg, TasqueTaskListArgument):
                ret = eval(arg.expr, eval_name_scope)
                assert isinstance(ret, list)
                evaled_param_args.extend(ret)
            elif isinstance(arg, TasqueTaskArgument):
                evaled_param_args.append(eval(arg.expr, eval_name_scope))
            else:
                raise ValueError("Unknown arg")
        else:
            raise ValueError("Unknown arg")
    evaled_param_kwargs = {}
    for arg in param_kwargs:
        if isinstance(arg, TasqueTaskDictArgument):
            if eval(arg.cond, eval_name_scope):
                evaled_param_kwargs.update(eval(arg.expr, eval_name_scope))
        else:
            for key, subarg in arg.items():
                if isinstance(subarg, str):
                    evaled_param_kwargs[key] = subarg
                elif eval(subarg.cond, eval_name_scope):
                    if isinstance(subarg, TasqueTaskFstrArgument):
                        evaled_param_kwargs[key] = eval(f'f"""{subarg.expr}"""', eval_name_scope)
                    elif isinstance(subarg, TasqueTaskArgument):
                        evaled_param_kwargs[key] = eval(subarg.expr, eval_name_scope)
                    else:
                        raise ValueError("Unknown arg")
                else:
                    raise ValueError("Unknown arg")
    return evaled_param_args, evaled_param_kwargs
