import os
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Extra, Field


class TasqueTaskStatus(str, Enum):
    CREATED = 'created'
    PREPARED = 'prepared'
    PENDING = 'pending'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    PAUSED = 'paused'

class TasqueTaskArgument(BaseModel):
    expr: str
    cond: Optional[str] = "True"

    class Config:
        extra = Extra.forbid

class TasqueTaskFstrArgument(BaseModel):
    type: Literal["fstr"]
    expr: str
    cond: Optional[str] = "True"

class TasqueTaskListArgument(BaseModel):
    type: Literal["list"]
    expr: str
    cond: Optional[str] = "True"

class TasqueTaskDictArgument(BaseModel):
    type: Literal["dict"]
    expr: str
    cond: Optional[str] = "True"

class TasqueExternalStateDependency(BaseModel):
    expr: str

class TasqueExternalAcquisition(BaseModel):
    key: str

class TasqueRetry(BaseModel):
    count: int = 0
    wait: int = 1

class TasqueBaseTask(BaseModel):
    name: Optional[str]
    msg: Optional[str]
    dependencies: Optional[List[
        Union[int, TasqueExternalStateDependency]
    ]]
    groups: Optional[List[Union[str, TasqueExternalAcquisition]]]
    retry: Optional[TasqueRetry] = TasqueRetry()
    env: Optional[Dict[str, str]]
    log: Optional[str]
    result: Optional[Any]
    status: Optional[str]
    status_data: Optional[Dict[str, Any]]

class TasqueSubprocessTask(TasqueBaseTask):
    type: str = Field("subprocess", const=True)
    cwd: Optional[str]
    cmd: Optional[str]
    options: Optional[
        List[
            Union[
                str,
                TasqueTaskDictArgument,
                TasqueTaskListArgument,
                TasqueTaskFstrArgument,
                TasqueTaskArgument,
                Dict[
                    str,
                    Union[str, TasqueTaskListArgument, TasqueTaskFstrArgument, TasqueTaskArgument],
                ],
            ]
        ]
    ]
    evaled_cmd: Optional[List[str]]
    evaled_cmdline: Optional[str]
    evaled_options: Optional[List[Any]]

class TasqueShellTask(TasqueBaseTask):
    type: str = Field("sh", const=True)
    cwd: Optional[str]
    shell: Optional[str]
    script: Optional[str]
    evaled_script: Optional[str]

class TasqueFunctionTask(TasqueBaseTask):
    type: str = Field("function", const=True)
    func: Optional[str]
    args: Optional[
        List[Union[str, TasqueTaskListArgument, TasqueTaskFstrArgument, TasqueTaskArgument]]
    ]
    kwargs: Optional[
        List[
            Union[
                TasqueTaskDictArgument,
                Dict[str, Union[str, TasqueTaskFstrArgument, TasqueTaskArgument]],
            ]
        ]
    ]
    evaled_args: Optional[List[Any]]
    evaled_kwargs: Optional[Dict[str, Any]]

class TasqueSpecification(BaseModel):
    name: Optional[str]
    global_params: Optional[Dict[str, Any]]
    global_env: Optional[Dict[str, str]]
    root_dir: Optional[str]
    groups: Optional[Dict[str, int]]
    tasks: Optional[Dict[int, Union[TasqueSubprocessTask, TasqueShellTask, TasqueFunctionTask]]]
