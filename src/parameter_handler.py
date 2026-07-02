"""
PySpark Parameter Handler Module
Replaces ABAP selection screen with argparse-based parameter management
"""
import argparse
import yaml
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from pathlib import Path


class ParameterHandler:
    """Handles command-line parameters and configuration for ETL process"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize parameter handler
        
        Args:
            config_path: Path to YAML configuration file
        """
        self.config_path = config_path or "config.yaml"
        self.config = self._load_config()
        self.params = {}
        
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        config_file = Path(self.config_path)
        if config_file.exists():
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        return {}
    
    def parse_arguments(self) -> Dict[str, Any]:
        """
        Parse command-line arguments (replaces ABAP selection screen)
        
        Returns:
            Dictionary of parsed parameters
        """
        parser = argparse.ArgumentParser(
            description='Sales Data ETL Process - PySpark Implementation',
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  # Process last 7 days
  python main.py --from-date 2024-01-01 --to-date 2024-01-07
  
  # Test mode
  python main.py --from-date 2024-01-01 --to-date 2024-01-07 --test-mode
  
  # Custom batch size
  python main.py --from-date 2024-01-01 --to-date 2024-01-07 --batch-size 2000
            """
        )
        
        # Date parameters (replaces p_fdate, p_tdate from ABAP)
        parser.add_argument(
            '--from-date',
            type=str,
            required=False,
            default=(datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            help='Start date for ETL processing (format: YYYY-MM-DD). Default: 7 days ago'
        )
        
        parser.add_argument(
            '--to-date',
            type=str,
            required=False,
            default=datetime.now().strftime('%Y-%m-%d'),
            help='End date for ETL processing (format: YYYY-MM-DD). Default: today'
        )
        
        # Test mode parameter (replaces p_test from ABAP)
        parser.add_argument(
            '--test-mode',
            action='store_true',
            default=False,
            help='Run in test mode (no data committed)'
        )
        
        # ETL configuration parameters
        parser.add_argument(
            '--batch-size',
            type=int,
            default=self.config.get('etl', {}).get('batch_size', 1000),
            help='Batch size for processing records'
        )
        
        parser.add_argument(
            '--commit-interval',
            type=int,
            default=self.config.get('etl', {}).get('commit_interval', 500),
            help='Number of records before commit'
        )
        
        parser.add_argument(
            '--parallel-jobs',
            type=int,
            default=self.config.get('spark', {}).get('executor_instances', 2),
            help='Number of parallel processing jobs'
        )
        
        parser.add_argument(
            '--retry-attempts',
            type=int,
            default=self.config.get('etl', {}).get('retry_attempts', 3),
            help='Number of retry attempts on failure'
        )
        
        # Data source parameters
        parser.add_argument(
            '--input-path',
            type=str,
            default=self.config.get('paths', {}).get('raw_data', 'data/raw'),
            help='Input data path'
        )
        
        parser.add_argument(
            '--output-path',
            type=str,
            default=self.config.get('paths', {}).get('analytics', 'data/analytics'),
            help='Output data path'
        )
        
        parser.add_argument(
            '--log-path',
            type=str,
            default=self.config.get('paths', {}).get('logs', 'logs'),
            help='Log file path'
        )
        
        # Configuration file override
        parser.add_argument(
            '--config',
            type=str,
            default=self.config_path,
            help='Path to configuration file'
        )
        
        args = parser.parse_args()
        
        # Store parsed parameters
        self.params = vars(args)
        
        # Validate parameters
        self._validate_parameters()
        
        return self.params
    
    def _validate_parameters(self) -> None:
        """
        Validate parsed parameters (replaces AT SELECTION-SCREEN in ABAP)
        
        Raises:
            ValueError: If parameter validation fails
        """
        # Validate and parse dates
        from_date = self._parse_date(self.params['from_date'])
        to_date = self._parse_date(self.params['to_date'])
        
        self.params['from_date_parsed'] = from_date
        self.params['to_date_parsed'] = to_date
        
        # Validate date range (replaces ABAP validation)
        if from_date > to_date:
            raise ValueError(
                f"From Date ({self.params['from_date']}) cannot be later than "
                f"To Date ({self.params['to_date']})"
            )
        
        # Validate to_date is not in future
        if to_date > datetime.now().date():
            raise ValueError(
                f"To Date ({self.params['to_date']}) cannot be in the future"
            )
        
        # Validate date range is reasonable (e.g., not more than 1 year)
        date_diff = (to_date - from_date).days
        max_days = self.config.get('validation', {}).get('max_date_range_days', 365)
        
        if date_diff > max_days:
            raise ValueError(
                f"Date range ({date_diff} days) exceeds maximum allowed "
                f"range of {max_days} days"
            )
        
        # Validate numeric parameters
        if self.params['batch_size'] <= 0:
            raise ValueError("Batch size must be positive")
        
        if self.params['commit_interval'] <= 0:
            raise ValueError("Commit interval must be positive")
        
        if self.params['parallel_jobs'] <= 0:
            raise ValueError("Number of parallel jobs must be positive")
        
        if self.params['retry_attempts'] < 0:
            raise ValueError("Retry attempts cannot be negative")
    
    def _parse_date(self, date_string: str) -> datetime.date:
        """
        Parse date string to date object
        
        Args:
            date_string: Date in YYYY-MM-DD format
            
        Returns:
            datetime.date object
            
        Raises:
            ValueError: If date format is invalid
        """
        try:
            return datetime.strptime(date_string, '%Y-%m-%d').date()
        except ValueError:
            raise ValueError(
                f"Invalid date format: {date_string}. "
                f"Expected format: YYYY-MM-DD"
            )
    
    def get_parameter(self, key: str, default: Any = None) -> Any:
        """
        Get parameter value by key
        
        Args:
            key: Parameter key
            default: Default value if key not found
            
        Returns:
            Parameter value
        """
        return self.params.get(key, default)
    
    def get_all_parameters(self) -> Dict[str, Any]:
        """
        Get all parameters
        
        Returns:
            Dictionary of all parameters
        """
        return self.params.copy()
    
    def display_parameters(self) -> str:
        """
        Create formatted parameter display (replaces ABAP display)
        
        Returns:
            Formatted parameter string
        """
        output = []
        output.append("=" * 70)
        output.append("")
        output.append("Sales Data ETL Process - Parameter Summary".center(70))
        output.append("")
        output.append("=" * 70)
        output.append("")
        output.append(f"Processing Date Range: {self.params['from_date']} to {self.params['to_date']}")
        output.append(f"Test Mode: {'Yes' if self.params['test_mode'] else 'No'}")
        output.append(f"Batch Size: {self.params['batch_size']}")
        output.append(f"Commit Interval: {self.params['commit_interval']}")
        output.append(f"Parallel Jobs: {self.params['parallel_jobs']}")
        output.append(f"Input Path: {self.params['input_path']}")
        output.append(f"Output Path: {self.params['output_path']}")
        output.append(f"Log Path: {self.params['log_path']}")
        output.append("")
        output.append("=" * 70)
        
        return "\n".join(output)


class DateValidator:
    """Date validation utilities (replaces ABAP date validation logic)"""
    
    @staticmethod
    def validate_date_format(date_string: str) -> bool:
        """
        Validate date format
        
        Args:
            date_string: Date string to validate
            
        Returns:
            True if valid format, False otherwise
        """
        try:
            datetime.strptime(date_string, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    @staticmethod
    def validate_date_range(from_date: datetime.date, 
                          to_date: datetime.date) -> tuple[bool, str]:
        """
        Validate date range
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        if from_date > to_date:
            return False, "From date cannot be later than to date"
        
        if to_date > datetime.now().date():
            return False, "To date cannot be in the future"
        
        if from_date > datetime.now().date():
            return False, "From date cannot be in the future"
        
        return True, ""
    
    @staticmethod
    def is_business_day(date: datetime.date) -> bool:
        """
        Check if date is a business day (Monday-Friday)
        
        Args:
            date: Date to check
            
        Returns:
            True if business day
        """
        return date.weekday() < 5
    
    @staticmethod
    def get_date_range(from_date: datetime.date, 
                       to_date: datetime.date) -> list[datetime.date]:
        """
        Get list of dates in range
        
        Args:
            from_date: Start date
            to_date: End date
            
        Returns:
            List of dates
        """
        dates = []
        current = from_date
        while current <= to_date:
            dates.append(current)
            current += timedelta(days=1)
        return dates
    
    @staticmethod
    def format_date_for_spark(date: datetime.date) -> str:
        """
        Format date for Spark SQL queries
        
        Args:
            date: Date to format
            
        Returns:
            Formatted date string
        """
        return date.strftime('%Y-%m-%d')
    
    @staticmethod
    def format_date_for_display(date: datetime.date) -> str:
        """
        Format date for display
        
        Args:
            date: Date to format
            
        Returns:
            Formatted date string
        """
        return date.strftime('%Y-%m-%d')
    
    @staticmethod
    def parse_abap_date(abap_date: str) -> datetime.date:
        """
        Parse ABAP DATS format (YYYYMMDD) to Python date
        
        Args:
            abap_date: ABAP date string
            
        Returns:
            datetime.date object
        """
        return datetime.strptime(abap_date, '%Y%m%d').date()
    
    @staticmethod
    def to_abap_date(date: datetime.date) -> str:
        """
        Convert Python date to ABAP DATS format (YYYYMMDD)
        
        Args:
            date: Python date
            
        Returns:
            ABAP format date string
        """
        return date.strftime('%Y%m%d')