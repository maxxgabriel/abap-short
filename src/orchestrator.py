"""
ETL Orchestrator Module
Coordinates the complete ETL process.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Any, Tuple

from src.extract import ETLExtractor
from src.transform import ETLTransformer
from src.load import ETLLoader
from src.logger import ETLLogger
from src.exceptions import ETLError


class ETLOrchestrator:
    """Orchestrates the complete ETL workflow."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the ETL Orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(spark, self.etl_run_id)
        
        # Initialize ETL components
        self.extractor = ETLExtractor(spark, self.logger, config)
        self.transformer = ETLTransformer(spark, self.logger, config)
        self.loader = ETLLoader(spark, self.logger, config)
        
        # Track execution time
        self.start_time = None
        self.end_time = None
        
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
    
    def run_etl(
        self,
        source_table: str,
        target_table: str,
        from_date: str,
        to_date: str
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute the complete ETL process.
        
        Args:
            source_table: Source table name
            target_table: Target analytics table name
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            
        Returns:
            Tuple of (success flag, execution statistics)
        """
        try:
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time}'
            )
            
            print(f"\n{'='*70}")
            print(f"{'ETL Process Execution':^70}")
            print(f"{'='*70}")
            print(f"Run ID: {self.etl_run_id}")
            print(f"Date Range: {from_date} to {to_date}")
            print(f"{'='*70}\n")
            
            # Step 1: Extract
            print("=== EXTRACT Phase ===")
            raw_df = self.extractor.extract_data(
                source_table=source_table,
                from_date=from_date,
                to_date=to_date,
                status_filter='N'
            )
            
            if raw_df.count() == 0:
                self.logger.log_message(
                    step='COMPLETE',
                    status='W',
                    message='No records to process'
                )
                return True, {'total_records': 0}
            
            # Step 2: Transform
            print("\n=== TRANSFORM Phase ===")
            analytics_df = self.transformer.transform_data(
                raw_df=raw_df,
                etl_run_id=self.etl_run_id
            )
            
            # Step 3: Load
            print("\n=== LOAD Phase ===")
            success, load_stats = self.loader.load_data(
                analytics_df=analytics_df,
                source_table=source_table,
                target_table=target_table
            )
            
            self.end_time = datetime.now()
            
            if success:
                duration = (self.end_time - self.start_time).total_seconds()
                
                self.logger.log_message(
                    step='COMPLETE',
                    status='S',
                    message=f'ETL process completed successfully at {self.end_time}'
                )
                
                stats = {
                    **load_stats,
                    'etl_run_id': self.etl_run_id,
                    'start_time': self.start_time.isoformat(),
                    'end_time': self.end_time.isoformat(),
                    'duration_seconds': duration
                }
                
                self._display_summary(stats)
                
                return True, stats
            else:
                raise ETLError('ETL process completed with errors')
                
        except Exception as e:
            self.end_time = datetime.now()
            
            self.logger.log_message(
                step='ERROR',
                status='E',
                message=f'ETL process failed: {str(e)}'
            )
            
            print(f"\n*** ETL Process Failed ***")
            print(f"Error: {str(e)}")
            
            return False, {'error': str(e)}
    
    def _generate_etl_run_id(self) -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f'ETL{timestamp}'
    
    def _display_summary(self, stats: Dict[str, Any]):
        """
        Display execution summary.
        
        Args:
            stats: Execution statistics dictionary
        """
        print(f"\n{'='*70}")
        print(f"{'ETL Process Summary':^70}")
        print(f"{'='*70}")
        print(f"ETL Run ID:        {stats['etl_run_id']}")
        print(f"Start Time:        {stats['start_time']}")
        print(f"End Time:          {stats['end_time']}")
        print(f"Duration:          {stats['duration_seconds']:.2f} seconds")
        print(f"\nRecord Statistics:")
        print(f"  Total Records:   {stats.get('total_records', 0)}")
        print(f"  Valid Records:   {stats.get('valid_records', 0)}")
        print(f"  Invalid Records: {stats.get('invalid_records', 0)}")
        print(f"  Loaded Records:  {stats.get('loaded_records', 0)}")
        print(f"{'='*70}\n")