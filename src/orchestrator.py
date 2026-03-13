"""
ETL Orchestrator Module
Coordinates the complete ETL process.
"""

import logging
from datetime import datetime, date
from typing import Dict, Tuple
from pyspark.sql import SparkSession

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader
from src.logger import ETLLogger

logger = logging.getLogger(__name__)


class ETLOrchestrator:
    """Main orchestrator for the ETL process."""
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.run_id = self._generate_run_id()
        self.etl_logger = ETLLogger(self.run_id, config)
        
        # Initialize components
        self.extractor = SalesDataExtractor(spark, config)
        self.transformer = SalesDataTransformer(config)
        self.loader = SalesDataLoader(config)
        
        self.start_time = None
        self.end_time = None
    
    def _generate_run_id(self) -> str:
        """Generate unique ETL run ID."""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        return f"ETL{timestamp}"
    
    def run_etl(
        self,
        from_date: date,
        to_date: date,
        test_mode: bool = False
    ) -> Tuple[bool, Dict]:
        """
        Execute complete ETL process.
        
        Args:
            from_date: Start date for extraction
            to_date: End date for extraction
            test_mode: If True, use sample data and skip database writes
            
        Returns:
            Tuple of (success, statistics_dict)
        """
        statistics = {
            'run_id': self.run_id,
            'from_date': str(from_date),
            'to_date': str(to_date),
            'test_mode': test_mode,
            'extracted': 0,
            'transformed': 0,
            'loaded': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None,
            'duration_seconds': 0
        }
        
        try:
            self.start_time = datetime.now()
            statistics['start_time'] = self.start_time.isoformat()
            
            logger.info(f"[{self.run_id}] ETL process started at {self.start_time}")
            self.etl_logger.log_step('INIT', 'S', f"ETL process initialized with run ID: {self.run_id}")
            
            # Phase 1: Extract
            logger.info(f"[{self.run_id}] === EXTRACT Phase ===")
            self.etl_logger.log_step('EXTRACT', 'S', 'Starting data extraction')
            
            if test_mode:
                raw_df = self.extractor.create_sample_data(self.run_id)
            else:
                raw_df = self.extractor.extract_data(from_date, to_date, self.run_id)
            
            if raw_df is None:
                raise RuntimeError("Extraction failed")
            
            statistics['extracted'] = raw_df.count()
            self.etl_logger.log_step(
                'EXTRACT', 'S',
                f"Extracted {statistics['extracted']} records",
                records_processed=statistics['extracted'],
                records_success=statistics['extracted']
            )
            
            # Phase 2: Transform
            logger.info(f"[{self.run_id}] === TRANSFORM Phase ===")
            self.etl_logger.log_step('TRANSFORM', 'S', 'Starting data transformation')
            
            analytics_df = self.transformer.transform_data(raw_df, self.run_id)
            statistics['transformed'] = analytics_df.count()
            
            self.etl_logger.log_step(
                'TRANSFORM', 'S',
                f"Transformed {statistics['transformed']} records",
                records_processed=statistics['transformed'],
                records_success=statistics['transformed']
            )
            
            # Phase 3: Load
            logger.info(f"[{self.run_id}] === LOAD Phase ===")
            self.etl_logger.log_step('LOAD', 'S', 'Starting data load')
            
            load_success, loaded_count, failed_count = self.loader.load_data(
                analytics_df, self.run_id, test_mode
            )
            
            if not load_success:
                raise RuntimeError("Load failed")
            
            statistics['loaded'] = loaded_count
            statistics['failed'] = failed_count
            
            self.etl_logger.log_step(
                'LOAD', 'S',
                f"Loaded {loaded_count} records, {failed_count} failed",
                records_processed=loaded_count + failed_count,
                records_success=loaded_count,
                records_error=failed_count
            )
            
            # Completion
            self.end_time = datetime.now()
            statistics['end_time'] = self.end_time.isoformat()
            statistics['duration_seconds'] = (self.end_time - self.start_time).total_seconds()
            
            logger.info(f"[{self.run_id}] ETL process completed successfully at {self.end_time}")
            self.etl_logger.log_step('COMPLETE', 'S', 'ETL process completed successfully')
            
            return True, statistics
            
        except Exception as e:
            self.end_time = datetime.now()
            statistics['end_time'] = self.end_time.isoformat()
            if self.start_time:
                statistics['duration_seconds'] = (self.end_time - self.start_time).total_seconds()
            
            error_msg = f"ETL process failed: {str(e)}"
            logger.error(f"[{self.run_id}] {error_msg}", exc_info=True)
            self.etl_logger.log_step('ERROR', 'E', error_msg)
            
            return False, statistics
    
    def get_summary(self, statistics: Dict) -> str:
        """
        Generate summary report.
        
        Args:
            statistics: Statistics dictionary from run_etl
            
        Returns:
            Formatted summary string
        """
        summary = [
            "=" * 70,
            "ETL Process Summary",
            "=" * 70,
            f"ETL Run ID:       {statistics['run_id']}",
            f"Date Range:       {statistics['from_date']} to {statistics['to_date']}",
            f"Test Mode:        {statistics['test_mode']}",
            f"Start Time:       {statistics['start_time']}",
            f"End Time:         {statistics['end_time']}",
            f"Duration:         {statistics['duration_seconds']:.2f} seconds",
            "",
            "Records Processed:",
            f"  Extracted:      {statistics['extracted']}",
            f"  Transformed:    {statistics['transformed']}",
            f"  Loaded:         {statistics['loaded']}",
            f"  Failed:         {statistics['failed']}",
            "=" * 70
        ]
        return "\n".join(summary)