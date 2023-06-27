import threading
from typing import List

from tasque.models import TasqueExternalAcquisition, TasqueExternalStateDependency


class TasqueCommunicator:
    def __init__(self) -> None:
        self._lock = threading.Lock()
    
    @property
    def lock(self):
        return self._lock
    
    def satisfied(self, eid, tid, dep: TasqueExternalStateDependency) -> bool:
        return True
    
    def acquire_clearance_to_run(self, eid, tid, deps: List[TasqueExternalAcquisition]) -> bool:
        return True
    
    def release_acquired(self, eid, tid, deps: List[TasqueExternalAcquisition]) -> bool:
        pass
    
    def is_clear_to_run(self, eid, tid, deps: List[TasqueExternalAcquisition]) -> bool:
        return True
