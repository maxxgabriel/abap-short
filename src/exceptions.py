"""
Exception hierarchy for ETL system with chaining support.

This module defines a comprehensive exception hierarchy for the ETL pipeline,
enabling root cause analysis through exception chaining and detailed error context.
"""

from typing import Optional, Dict, Any
from datetime import datetime


class ETLError(Exception):
    """
    Base exception class for all ETL errors.
    
    Supports exception chaining for root cause analysis and provides
    detailed error context including timestamp, step, and metadata.
    
    Attributes:
        message: Human-readable error message
        error_step: ETL step where error occurred (EXTRACT, TRANSFORM, LOAD)
        record_id: Optional identifier of the record that caused the error
        timestamp: When the error occurred
        error_code: Optional error classification code
        metadata: Additional context information
        original_exception: The underlying cause (for chaining)
    """
    
    def __init__(
        self,
        message: str,
        error_step: Optional[str] = None,
        record_id: Optional[str] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize ETL error with context.
        
        Args:
            message: Error description
            error_step: ETL pipeline step (EXTRACT/TRANSFORM/LOAD)
            record_id: ID of problematic record
            error_code: Classification code
            metadata: Additional error context
            original_exception: Root cause exception for chaining
        """
        super().__init__(message)
        self.message = message
        self.error_step = error_step
        self.record_id = record_id
        self.error_code = error_code
        self.metadata = metadata or {}
        self.timestamp = datetime.now()
        self.original_exception = original_exception
        
        # Maintain exception chain
        if original_exception:
            self.__cause__ = original_exception
    
    def get_full_context(self) -> Dict[str, Any]:
        """
        Get complete error context including chain.
        
        Returns:
            Dictionary with full error details and exception chain
        """
        context = {
            'error_type': self.__class__.__name__,
            'message': self.message,
            'error_step': self.error_step,
            'record_id': self.record_id,
            'error_code': self.error_code,
            'timestamp': self.timestamp.isoformat(),
            'metadata': self.metadata
        }
        
        # Include chained exception details
        if self.original_exception:
            context['root_cause'] = {
                'type': type(self.original_exception).__name__,
                'message': str(self.original_exception),
                'details': getattr(self.original_exception, 'args', ())
            }
        
        return context
    
    def __str__(self) -> str:
        """Format error message with context."""
        parts = [f"[{self.__class__.__name__}]"]
        
        if self.error_step:
            parts.append(f"Step: {self.error_step}")
        
        if self.record_id:
            parts.append(f"Record: {self.record_id}")
        
        parts.append(f"Message: {self.message}")
        
        if self.original_exception:
            parts.append(f"Root Cause: {type(self.original_exception).__name__}: {self.original_exception}")
        
        return " | ".join(parts)


class ExtractError(ETLError):
    """
    Exception raised during data extraction phase.
    
    Indicates failures in reading source data, connection issues,
    or data access problems.
    """
    
    def __init__(
        self,
        message: str,
        source_table: Optional[str] = None,
        record_id: Optional[str] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize extraction error.
        
        Args:
            message: Error description
            source_table: Source table/file name
            record_id: ID of problematic record
            error_code: Classification code
            metadata: Additional context
            original_exception: Root cause
        """
        enhanced_metadata = metadata or {}
        if source_table:
            enhanced_metadata['source_table'] = source_table
        
        super().__init__(
            message=message,
            error_step='EXTRACT',
            record_id=record_id,
            error_code=error_code or 'EXT_001',
            metadata=enhanced_metadata,
            original_exception=original_exception
        )
        self.source_table = source_table


class TransformError(ETLError):
    """
    Exception raised during data transformation phase.
    
    Indicates business rule violations, calculation errors,
    or data validation failures.
    """
    
    def __init__(
        self,
        message: str,
        transformation_rule: Optional[str] = None,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        invalid_value: Optional[Any] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize transformation error.
        
        Args:
            message: Error description
            transformation_rule: Failed business rule
            record_id: ID of problematic record
            field_name: Field that failed validation
            invalid_value: The invalid value
            error_code: Classification code
            metadata: Additional context
            original_exception: Root cause
        """
        enhanced_metadata = metadata or {}
        if transformation_rule:
            enhanced_metadata['transformation_rule'] = transformation_rule
        if field_name:
            enhanced_metadata['field_name'] = field_name
        if invalid_value is not None:
            enhanced_metadata['invalid_value'] = str(invalid_value)
        
        super().__init__(
            message=message,
            error_step='TRANSFORM',
            record_id=record_id,
            error_code=error_code or 'TRF_001',
            metadata=enhanced_metadata,
            original_exception=original_exception
        )
        self.transformation_rule = transformation_rule
        self.field_name = field_name
        self.invalid_value = invalid_value


class LoadError(ETLError):
    """
    Exception raised during data loading phase.
    
    Indicates target system failures, constraint violations,
    or write operation problems.
    """
    
    def __init__(
        self,
        message: str,
        target_table: Optional[str] = None,
        record_id: Optional[str] = None,
        constraint_violated: Optional[str] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize load error.
        
        Args:
            message: Error description
            target_table: Target table/location
            record_id: ID of problematic record
            constraint_violated: Constraint that failed
            error_code: Classification code
            metadata: Additional context
            original_exception: Root cause
        """
        enhanced_metadata = metadata or {}
        if target_table:
            enhanced_metadata['target_table'] = target_table
        if constraint_violated:
            enhanced_metadata['constraint_violated'] = constraint_violated
        
        super().__init__(
            message=message,
            error_step='LOAD',
            record_id=record_id,
            error_code=error_code or 'LOAD_001',
            metadata=enhanced_metadata,
            original_exception=original_exception
        )
        self.target_table = target_table
        self.constraint_violated = constraint_violated


class ValidationError(ETLError):
    """
    Exception raised during data validation.
    
    Indicates schema violations, data quality issues,
    or business rule failures.
    """
    
    def __init__(
        self,
        message: str,
        validation_rule: str,
        record_id: Optional[str] = None,
        field_name: Optional[str] = None,
        expected_value: Optional[Any] = None,
        actual_value: Optional[Any] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize validation error.
        
        Args:
            message: Error description
            validation_rule: Rule that was violated
            record_id: ID of problematic record
            field_name: Field that failed validation
            expected_value: Expected value/format
            actual_value: Actual value received
            error_code: Classification code
            metadata: Additional context
            original_exception: Root cause
        """
        enhanced_metadata = metadata or {}
        enhanced_metadata['validation_rule'] = validation_rule
        if field_name:
            enhanced_metadata['field_name'] = field_name
        if expected_value is not None:
            enhanced_metadata['expected_value'] = str(expected_value)
        if actual_value is not None:
            enhanced_metadata['actual_value'] = str(actual_value)
        
        super().__init__(
            message=message,
            error_step='VALIDATE',
            record_id=record_id,
            error_code=error_code or 'VAL_001',
            metadata=enhanced_metadata,
            original_exception=original_exception
        )
        self.validation_rule = validation_rule
        self.field_name = field_name
        self.expected_value = expected_value
        self.actual_value = actual_value


class ConfigurationError(ETLError):
    """
    Exception raised for configuration issues.
    
    Indicates missing or invalid configuration settings
    that prevent ETL execution.
    """
    
    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        config_value: Optional[Any] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize configuration error.
        
        Args:
            message: Error description
            config_key: Configuration parameter name
            config_value: Invalid configuration value
            error_code: Classification code
            metadata: Additional context
            original_exception: Root cause
        """
        enhanced_metadata = metadata or {}
        if config_key:
            enhanced_metadata['config_key'] = config_key
        if config_value is not None:
            enhanced_metadata['config_value'] = str(config_value)
        
        super().__init__(
            message=message,
            error_step='INIT',
            error_code=error_code or 'CFG_001',
            metadata=enhanced_metadata,
            original_exception=original_exception
        )
        self.config_key = config_key
        self.config_value = config_value


class DataQualityError(ETLError):
    """
    Exception raised for data quality issues.
    
    Indicates data that doesn't meet quality standards
    but may be processable with warnings.
    """
    
    def __init__(
        self,
        message: str,
        quality_rule: str,
        record_id: Optional[str] = None,
        quality_score: Optional[float] = None,
        threshold: Optional[float] = None,
        error_code: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        original_exception: Optional[Exception] = None
    ):
        """
        Initialize data quality error.
        
        Args:
            message: Error description
            quality_rule: Quality rule violated
            record_id: ID of problematic record
            quality_score: Calculated quality score
            threshold: Required quality threshold
            error_code: Classification code
            metadata: Additional context
            original_exception: Root cause
        """
        enhanced_metadata = metadata or {}
        enhanced_metadata['quality_rule'] = quality_rule
        if quality_score is not None:
            enhanced_metadata['quality_score'] = quality_score
        if threshold is not None:
            enhanced_metadata['threshold'] = threshold
        
        super().__init__(
            message=message,
            error_step='VALIDATE',
            record_id=record_id,
            error_code=error_code or 'DQ_001',
            metadata=enhanced_metadata,
            original_exception=original_exception
        )
        self.quality_rule = quality_rule
        self.quality_score = quality_score
        self.threshold = threshold