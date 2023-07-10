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

class TasqueBaseTask(BaseModel):
    name: str
    msg: Optional[str] = ""
    dependencies: List[
        Union[int, TasqueExternalStateDependency]
    ] = []
    groups: Optional[List[Union[str, TasqueExternalAcquisition]]] = ["default"]
    env: Optional[Dict[str, str]] = {}
    log: Optional[str] = None
    result: Optional[Any] = None
    status: Optional[str] = None
    status_data: Optional[Dict[str, Any]] = None

class TasqueSubprocessTask(TasqueBaseTask):
    type: Literal["subprocess"]
    cwd: str = "."
    cmd: str = "echo"
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
    ] = []
    evaled_cmd: Optional[List[str]] = None
    evaled_cmdline: Optional[str] = None
    evaled_options: Optional[List[Any]] = None

class TasqueShellTask(TasqueBaseTask):
    type: Literal["sh"]
    cwd: str = "."
    shell: str = "sh"
    script: str = "echo"
    evaled_script: Optional[str] = None

class TasqueFunctionTask(TasqueBaseTask):
    type: Literal["function"]
    func: str
    args: Optional[
        List[Union[str, TasqueTaskListArgument, TasqueTaskFstrArgument, TasqueTaskArgument]]
    ] = []
    kwargs: Optional[
        List[
            Union[
                TasqueTaskDictArgument,
                Dict[str, Union[str, TasqueTaskFstrArgument, TasqueTaskArgument]],
            ]
        ]
    ] = []
    evaled_args: Optional[List[Any]] = None
    evaled_kwargs: Optional[Dict[str, Any]] = None

class TasqueSpecification(BaseModel):
    name: str
    global_params: Optional[Dict[str, Any]] = {}
    global_env: Optional[Dict[str, str]] = {}
    root_dir: Optional[str] = os.environ.get("APP_SRC_DIR", ".")
    groups: Optional[Dict[str, int]] = {}
    tasks: Optional[Dict[int, Union[TasqueSubprocessTask, TasqueShellTask, TasqueFunctionTask]]] = {}
