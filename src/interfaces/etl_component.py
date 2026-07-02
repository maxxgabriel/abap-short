```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

@dataclass
class ExecutionResult:
    success: bool
    records_total: int = 0
    records_success: int = 0
    records_failed: int = 0
    message: str = ""
    error: Optional[Exception] = None

class ETLComponent(ABC):
    @abstractmethod
    def execute(self, *args, **kwargs) -> ExecutionResult:
        pass
    
    @abstractmethod
    def get_component_name(self) -> str:
        pass
    
    @abstractmethod
    def validate_prerequisites(self) -> bool:
        pass
```