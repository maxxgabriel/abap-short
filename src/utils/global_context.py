"""
Global ETL context manager.
Replaces global variables from ZETL_TOP.
"""
from typing import Optional
from datetime import datetime
from dataclasses import dataclass, field
from .common_types import ETLStatistics, ETLConfig


@dataclass
class ETLContext:
    """
    Global ETL execution context.
    Replaces: Global variables from ZETL_TOP
    """
    etl_run_id: str
    test_mode: bool = False
    batch_size: int = 1000
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    config: ETLConfig = field(default_factory=ETLConfig)
    statistics: ETLStatistics = field(default_factory=ETLStatistics)
    
    def __post_init__(self):
        """Initialize start time"""
        if self.start_time is None:
            self.start_time = datetime.now()
    
    def finalize(self):
        """Finalize execution and set end time"""
        self.end_time = datetime.now()
        self.statistics.start_time = self.start_time
        self.statistics.end_time = self.end_time
    
    @property
    def duration_seconds(self) -> float:
        """Get execution duration"""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0


class ContextManager:
    """Singleton context manager"""
    _instance: Optional[ETLContext] = None
    
    @classmethod
    def initialize(cls, etl_run_id: str, test_mode: bool = False, 
                  config: Optional[ETLConfig] = None) -> ETLContext:
        """Initialize global context"""
        cls._instance = ETLContext(
            etl_run_id=etl_run_id,
            test_mode=test_mode,
            config=config or ETLConfig()
        )
        return cls._instance
    
    @classmethod
    def get_context(cls) -> ETLContext:
        """Get current context"""
        if cls._instance is None:
            raise RuntimeError("ETL context not initialized")
        return cls._instance
    
    @classmethod
    def reset(cls):
        """Reset context"""
        cls._instance = None