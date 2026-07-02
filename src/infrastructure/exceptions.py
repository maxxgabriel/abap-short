```python
class ETLError(Exception):
    def __init__(self, message: str, error_step: str = None, record_id: str = None):
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        super().__init__(self.message)
    
    def __str__(self):
        error_msg = self.message
        if self.error_step:
            error_msg = f"[{self.error_step}] {error_msg}"
        if self.record_id:
            error_msg = f"{error_msg} (Record: {self.record_id})"
        return error_msg

class ExtractError(ETLError):
    pass

class TransformError(ETLError):
    pass

class LoadError(ETLError):
    pass

class ValidationError(ETLError):
    pass
```