"""
ETL Orchestrator Module
Coordinates the ETL process with exception-based error handling and UUID-based run ID generation.
Migrated from ABAP class ZCL_ETL_ORCHESTRATOR.
"""

import uuid
import logging
from datetime import datetime
from typing import Optional, Dict, Any
from pyspark.sql import SparkSession

from src.extract import Extractor
from src.transform import Transformer
from src.load import Loader
from src.logger import ETLLogger
from src.exceptions import ETLException, ExtractException, TransformException, LoadException


class ETLOrchestrator:
    """
    Main ETL orchestrator that coordinates the ETL process.
    
    Manages the complete ETL workflow including:
    - UUID-based run ID generation
    - Component initialization (extractor, transformer, loader)
    - Sequential execution of ETL phases
    - Exception-based error handling
    - Execution summary and metrics
    """

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        """
        Initialize the ETL orchestrator.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary containing ETL parameters
        """
        self.spark = spark
        self.config = config
        
        # Generate unique ETL run ID using UUID
        self.etl_run_id = self._generate_etl_run_id()
        
        # Initialize logger
        self.logger = ETLLogger(
            etl_run_id=self.etl_run_id,
            spark=spark,
            config=config
        )
        
        # Initialize ETL components
        self.extractor = Extractor(
            spark=spark,
            logger=self.logger,
            config=config
        )
        
        self.transformer = Transformer(
            spark=spark,
            logger=self.logger,
            config=config
        )
        
        self.loader = Loader(
            spark=spark,
            logger=self.logger,
            config=config
        )
        
        # Execution metrics
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.metrics: Dict[str, Any] = {}
        
        # Log initialization
        self.logger.log_message(
            step='INIT',
            status='S',
            message=f'ETL process initialized with run ID: {self.etl_run_id}'
        )
        
        logging.info(f'ETL Orchestrator initialized with run ID: {self.etl_run_id}')

    def _generate_etl_run_id(self) -> str:
        """
        Generate unique ETL run ID using UUID.
        
        Returns:
            String ETL run ID in format: ETL_{uuid}
        """
        # Generate UUID and format as ETL run ID
        run_uuid = str(uuid.uuid4()).replace('-', '')[:16].upper()
        etl_run_id = f'ETL_{run_uuid}'
        
        return etl_run_id

    def run_etl(self, from_date: str, to_date: str) -> bool:
        """
        Execute the complete ETL process.
        
        Args:
            from_date: Start date for data extraction (YYYY-MM-DD)
            to_date: End date for data extraction (YYYY-MM-DD)
            
        Returns:
            Boolean indicating success (True) or failure (False)
            
        Raises:
            ETLException: Base exception for all ETL errors
            ExtractException: Specific to extraction phase
            TransformException: Specific to transformation phase
            LoadException: Specific to loading phase
        """
        try:
            # Capture start time
            self.start_time = datetime.now()
            
            self.logger.log_message(
                step='START',
                status='S',
                message=f'ETL process started at {self.start_time.isoformat()}'
            )
            
            logging.info(f'=== Starting ETL Process ===')
            logging.info(f'Date Range: {from_date} to {to_date}')
            
            # ===================================================================
            # PHASE 1: EXTRACT
            # ===================================================================
            logging.info('=== EXTRACT Phase ===')
            
            raw_data_df = self.extractor.extract_data(
                from_date=from_date,
                to_date=to_date
            )
            
            if raw_data_df is None or raw_data_df.count() == 0:
                raise ExtractException(
                    error_text='No data extracted from source',
                    error_step='EXTRACT'
                )
            
            extract_count = raw_data_df.count()
            self.metrics['extracted_records'] = extract_count
            logging.info(f'Extracted {extract_count} records')
            
            # ===================================================================
            # PHASE 2: TRANSFORM
            # ===================================================================
            logging.info('=== TRANSFORM Phase ===')
            
            analytics_df = self.transformer.transform_data(
                raw_data_df=raw_data_df,
                etl_run_id=self.etl_run_id
            )
            
            if analytics_df is None or analytics_df.count() == 0:
                raise TransformException(
                    error_text='Transformation produced no results',
                    error_step='TRANSFORM'
                )
            
            transform_count = analytics_df.count()
            self.metrics['transformed_records'] = transform_count
            logging.info(f'Transformed {transform_count} records')
            
            # ===================================================================
            # PHASE 3: LOAD
            # ===================================================================
            logging.info('=== LOAD Phase ===')
            
            load_metrics = self.loader.load_data(
                analytics_df=analytics_df
            )
            
            self.metrics['loaded_records'] = load_metrics['loaded_records']
            self.metrics['failed_records'] = load_metrics['failed_records']
            logging.info(f"Loaded {load_metrics['loaded_records']} records")
            
            # ===================================================================
            # COMPLETION
            # ===================================================================
            self.end_time = datetime.now()
            duration = (self.end_time - self.start_time).total_seconds()
            self.metrics['duration_seconds'] = duration
            
            self.logger.log_message(
                step='COMPLETE',
                status='S',
                records_processed=extract_count,
                records_success=self.metrics['loaded_records'],
                records_error=self.metrics['failed_records'],
                message=f'ETL process completed successfully at {self.end_time.isoformat()}'
            )
            
            logging.info(f'=== ETL Process Completed Successfully ===')
            logging.info(f'Duration: {duration:.2f} seconds')
            
            return True
            
        except ExtractException as ex:
            self._handle_etl_error(ex, 'EXTRACT')
            raise
            
        except TransformException as ex:
            self._handle_etl_error(ex, 'TRANSFORM')
            raise
            
        except LoadException as ex:
            self._handle_etl_error(ex, 'LOAD')
            raise
            
        except Exception as ex:
            # Wrap unexpected exceptions in ETLException
            etl_ex = ETLException(
                error_text=f'Unexpected error: {str(ex)}',
                error_step='ORCHESTRATOR'
            )
            self._handle_etl_error(etl_ex, 'ERROR')
            raise etl_ex from ex

    def _handle_etl_error(self, exception: ETLException, step: str) -> None:
        """
        Handle ETL errors with logging and cleanup.
        
        Args:
            exception: The ETL exception that occurred
            step: The ETL step where the error occurred
        """
        self.end_time = datetime.now()
        
        error_message = f'{step} failed: {exception.error_text}'
        
        self.logger.log_message(
            step=step,
            status='E',
            message=error_message
        )
        
        logging.error(f'=== ETL Process Failed ===')
        logging.error(f'Step: {step}')
        logging.error(f'Error: {exception.error_text}')
        
        if self.start_time:
            duration = (self.end_time - self.start_time).total_seconds()
            logging.error(f'Failed after {duration:.2f} seconds')

    def get_etl_run_id(self) -> str:
        """
        Get the current ETL run ID.
        
        Returns:
            ETL run ID string
        """
        return self.etl_run_id

    def display_summary(self) -> Dict[str, Any]:
        """
        Generate and display execution summary.
        
        Returns:
            Dictionary containing execution summary metrics
        """
        summary = {
            'etl_run_id': self.etl_run_id,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.metrics.get('duration_seconds', 0),
            'extracted_records': self.metrics.get('extracted_records', 0),
            'transformed_records': self.metrics.get('transformed_records', 0),
            'loaded_records': self.metrics.get('loaded_records', 0),
            'failed_records': self.metrics.get('failed_records', 0)
        }
        
        # Log summary
        logging.info('=' * 60)
        logging.info('ETL Process Summary')
        logging.info('=' * 60)
        logging.info(f"ETL Run ID:    {summary['etl_run_id']}")
        logging.info(f"Start Time:    {summary['start_time']}")
        logging.info(f"End Time:      {summary['end_time']}")
        logging.info(f"Duration:      {summary['duration_seconds']:.2f} seconds")
        logging.info(f"Extracted:     {summary['extracted_records']} records")
        logging.info(f"Transformed:   {summary['transformed_records']} records")
        logging.info(f"Loaded:        {summary['loaded_records']} records")
        logging.info(f"Failed:        {summary['failed_records']} records")
        logging.info('=' * 60)
        
        return summary

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get execution metrics.
        
        Returns:
            Dictionary containing execution metrics
        """
        return self.metrics.copy()