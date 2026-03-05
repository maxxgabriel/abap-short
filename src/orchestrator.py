"""
ETL orchestration module for Sales ETL pipeline.
Coordinates extraction, transformation, and loading operations.
"""

from pyspark.sql import SparkSession
from datetime import datetime
from typing import Dict, Optional
import logging
import uuid

from src.extract import SalesDataExtractor
from src.transform import SalesDataTransformer
from src.load import SalesDataLoader


class ETLOrchestrator:
    """Orchestrates the complete ETL pipeline workflow."""
    
    def __init__(self, spark: SparkSession, config: dict):
        """
        Initialize the orchestrator.
        
        Args:
            spark: Active SparkSession
            config: Configuration dictionary
        """
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Generate unique ETL run ID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize components
        self.extractor = SalesDataExtractor(spark, config)
        self.transformer = SalesDataTransformer(spark, config)
        self.loader = SalesDataLoader(spark, config)
        
        # Execution metadata
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.execution_metrics: Dict = {}
    
    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run identifier.
        
        Returns:
            Unique ETL run ID
        """
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        unique_id = str(uuid.uuid4())[:8]
        return f"ETL_{timestamp}_{unique_id}"
    
    def run_etl(self, from_date: str, to_date: str) -> Dict:
        """
        Execute the complete ETL pipeline.
        
        Args:
            from_date: Start date for extraction (YYYY-MM-DD)
            to_date: End date for extraction (YYYY-MM-DD)
            
        Returns:
            Dictionary containing execution results and metrics
        """
        self.start_time = datetime.now()
        self.logger.info(f"Starting ETL process {self.etl_run_id} at {self.start_time}")
        
        try:
            # Phase 1: Extract
            self.logger.info("=== EXTRACT Phase ===")
            raw_df, extract_metrics = self.extractor.extract_data(from_date, to_date)
            self.execution_metrics['extract'] = extract_metrics
            
            # Validate extracted data
            is_valid, validation_msgs = self.extractor.validate_extracted_data(raw_df)
            if not is_valid:
                raise ValueError(f"Extraction validation failed: {validation_msgs}")
            
            # Phase 2: Transform
            self.logger.info("=== TRANSFORM Phase ===")
            analytics_df, transform_metrics = self.transformer.transform_data(
                raw_df, 
                self.etl_run_id
            )
            self.execution_metrics['transform'] = transform_metrics
            
            # Validate transformed data
            is_valid, validation_msgs = self.transformer.validate_transformed_data(analytics_df)
            if not is_valid:
                raise ValueError(f"Transformation validation failed: {validation_msgs}")
            
            # Phase 3: Load
            self.logger.info("=== LOAD Phase ===")
            load_success, load_metrics = self.loader.load_data(analytics_df)
            self.execution_metrics['load'] = load_metrics
            
            if not load_success:
                raise RuntimeError("Data load failed")
            
            # Update source status
            trans_ids = [row.trans_id for row in raw_df.select('trans_id').collect()]
            self.loader.update_source_status(trans_ids, 'P')
            
            # Complete successfully
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            
            result = {
                'success': True,
                'etl_run_id': self.etl_run_id,
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat(),
                'duration_seconds': duration,
                'metrics': self.execution_metrics
            }
            
            self.logger.info(f"ETL process completed successfully in {duration:.2f} seconds")
            return result
            
        except Exception as e:
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds() if self.start_time else 0
            
            error_result = {
                'success': False,
                'etl_run_id': self.etl_run_id,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat(),
                'duration_seconds': duration,
                'error': str(e),
                'metrics': self.execution_metrics
            }
            
            self.logger.error(f"ETL process failed: {str(e)}")
            return error_result
    
    def get_execution_summary(self) -> Dict:
        """
        Get summary of ETL execution.
        
        Returns:
            Dictionary with execution summary
        """
        if not self.start_time:
            return {'status': 'Not started'}
        
        duration = (self.end_time - self.start_time).total_seconds() if self.end_time else None
        
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else 'In progress',
            'duration_seconds': duration,
            'metrics': self.execution_metrics
        }
        
        return summary