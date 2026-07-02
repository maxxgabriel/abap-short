"""
ID Generation Utilities
Generates unique identifiers for ETL runs, logs, and analytics records
"""

from datetime import datetime
import uuid


class IDGenerator:
    """Utility class for generating unique IDs"""
    
    @staticmethod
    def generate_etl_run_id() -> str:
        """
        Generate unique ETL run ID
        Format: ETL_YYYYMMDDHHMMSS_UUID
        
        Returns:
            Unique ETL run ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"ETL_{timestamp}_{short_uuid}"
    
    @staticmethod
    def generate_log_id() -> str:
        """
        Generate unique log ID
        Format: LOG_YYYYMMDDHHMMSS_UUID
        
        Returns:
            Unique log ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        short_uuid = str(uuid.uuid4())[:8]
        return f"LOG_{timestamp}_{short_uuid}"
    
    @staticmethod
    def generate_analytics_id(trans_id: str) -> str:
        """
        Generate unique analytics ID
        Format: ANL_TRANSID_TIMESTAMP
        
        Args:
            trans_id: Original transaction ID
        
        Returns:
            Unique analytics ID
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        return f"ANL_{trans_id}_{timestamp}"