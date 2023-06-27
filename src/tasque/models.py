import os
from enum import Enum, auto
from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, Extra, Field


class TasqueTaskStatus(Enum):
    CREATED = auto()
    PREPARED = auto()
    PENDING = auto()
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    CANCELLED = auto()

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

class TasqueSubprocessTask(TasqueBaseTask):
    type: str = Field("subprocess", const=True)
    cwd: str = "."
    cmd: str = "echo"
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

class TasqueShellTask(TasqueBaseTask):
    type: str = Field("sh", const=True)
    cwd: str = "."
    shell: str = "sh"
    script: str = "echo"

class TasqueFunctionTask(TasqueBaseTask):
    type: str = Field("function", const=True)
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

class TasqueSpecification(BaseModel):
    name: str
    global_params: Optional[Dict[str, Any]] = {}
    global_env: Optional[Dict[str, str]] = {}
    root_dir: Optional[str] = os.environ.get("APP_SRC_DIR", ".")
    groups: Optional[Dict[str, int]] = {}
    tasks: Optional[Dict[int, Union[TasqueSubprocessTask, TasqueShellTask, TasqueFunctionTask]]] = {}
