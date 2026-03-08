"""
Unit tests for Parameter Handler and Date Validation
Tests parameter parsing, validation, and date utilities
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.parameter_handler import ParameterHandler, DateValidator


class TestParameterHandler:
    """Test suite for ParameterHandler class"""
    
    def test_init_with_default_config(self):
        """Test initialization with default config path"""
        handler = ParameterHandler()
        assert handler.config_path == "config.yaml"
        assert isinstance(handler.config, dict)
    
    def test_init_with_custom_config(self):
        """Test initialization with custom config path"""
        handler = ParameterHandler(config_path="custom_config.yaml")
        assert handler.config_path == "custom_config.yaml"
    
    @patch('sys.argv', ['test', '--from-date', '2024-01-01', '--to-date', '2024-01-31'])
    def test_parse_arguments_with_dates(self):
        """Test parsing command-line arguments with dates"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        assert params['from_date'] == '2024-01-01'
        assert params['to_date'] == '2024-01-31'
        assert params['from_date_parsed'] == datetime(2024, 1, 1).date()
        assert params['to_date_parsed'] == datetime(2024, 1, 31).date()
    
    @patch('sys.argv', ['test', '--test-mode'])
    def test_parse_arguments_test_mode(self):
        """Test parsing test mode flag"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        assert params['test_mode'] is True
    
    @patch('sys.argv', ['test', '--batch-size', '2000', '--commit-interval', '1000'])
    def test_parse_arguments_with_numeric_params(self):
        """Test parsing numeric parameters"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        assert params['batch_size'] == 2000
        assert params['commit_interval'] == 1000
    
    @patch('sys.argv', ['test', '--from-date', '2024-01-31', '--to-date', '2024-01-01'])
    def test_validate_parameters_invalid_date_range(self):
        """Test validation with invalid date range (from > to)"""
        handler = ParameterHandler()
        
        with pytest.raises(ValueError, match="cannot be later than"):
            handler.parse_arguments()
    
    @patch('sys.argv', ['test', '--from-date', '2024-01-01', 
                        '--to-date', (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')])
    def test_validate_parameters_future_date(self):
        """Test validation with future date"""
        handler = ParameterHandler()
        
        with pytest.raises(ValueError, match="cannot be in the future"):
            handler.parse_arguments()
    
    @patch('sys.argv', ['test', '--batch-size', '0'])
    def test_validate_parameters_invalid_batch_size(self):
        """Test validation with invalid batch size"""
        handler = ParameterHandler()
        
        with pytest.raises(ValueError, match="Batch size must be positive"):
            handler.parse_arguments()
    
    def test_parse_date_valid(self):
        """Test date parsing with valid format"""
        handler = ParameterHandler()
        result = handler._parse_date('2024-01-15')
        
        assert result == datetime(2024, 1, 15).date()
    
    def test_parse_date_invalid_format(self):
        """Test date parsing with invalid format"""
        handler = ParameterHandler()
        
        with pytest.raises(ValueError, match="Invalid date format"):
            handler._parse_date('15-01-2024')
    
    @patch('sys.argv', ['test'])
    def test_get_parameter(self):
        """Test getting specific parameter"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        batch_size = handler.get_parameter('batch_size')
        assert isinstance(batch_size, int)
        
        # Test with default value
        missing = handler.get_parameter('missing_key', 'default')
        assert missing == 'default'
    
    @patch('sys.argv', ['test'])
    def test_get_all_parameters(self):
        """Test getting all parameters"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        all_params = handler.get_all_parameters()
        
        assert isinstance(all_params, dict)
        assert 'from_date' in all_params
        assert 'to_date' in all_params
        assert 'test_mode' in all_params
    
    @patch('sys.argv', ['test', '--from-date', '2024-01-01', '--to-date', '2024-01-31'])
    def test_display_parameters(self):
        """Test parameter display formatting"""
        handler = ParameterHandler()
        handler.parse_arguments()
        display = handler.display_parameters()
        
        assert 'Sales Data ETL Process' in display
        assert '2024-01-01' in display
        assert '2024-01-31' in display
        assert 'Test Mode' in display


class TestDateValidator:
    """Test suite for DateValidator class"""
    
    def test_validate_date_format_valid(self):
        """Test date format validation with valid date"""
        assert DateValidator.validate_date_format('2024-01-15') is True
        assert DateValidator.validate_date_format('2024-12-31') is True
    
    def test_validate_date_format_invalid(self):
        """Test date format validation with invalid date"""
        assert DateValidator.validate_date_format('15-01-2024') is False
        assert DateValidator.validate_date_format('2024/01/15') is False
        assert DateValidator.validate_date_format('invalid') is False
    
    def test_validate_date_range_valid(self):
        """Test date range validation with valid range"""
        from_date = datetime(2024, 1, 1).date()
        to_date = datetime(2024, 1, 31).date()
        
        is_valid, message = DateValidator.validate_date_range(from_date, to_date)
        assert is_valid is True
        assert message == ""
    
    def test_validate_date_range_invalid_order(self):
        """Test date range validation with invalid order"""
        from_date = datetime(2024, 1, 31).date()
        to_date = datetime(2024, 1, 1).date()
        
        is_valid, message = DateValidator.validate_date_range(from_date, to_date)
        assert is_valid is False
        assert "cannot be later than" in message
    
    def test_validate_date_range_future_date(self):
        """Test date range validation with future date"""
        from_date = datetime.now().date()
        to_date = datetime.now().date() + timedelta(days=1)
        
        is_valid, message = DateValidator.validate_date_range(from_date, to_date)
        assert is_valid is False
        assert "cannot be in the future" in message
    
    def test_is_business_day(self):
        """Test business day check"""
        # Monday (business day)
        monday = datetime(2024, 1, 1).date()
        assert DateValidator.is_business_day(monday) is True
        
        # Saturday (weekend)
        saturday = datetime(2024, 1, 6).date()
        assert DateValidator.is_business_day(saturday) is False
        
        # Sunday (weekend)
        sunday = datetime(2024, 1, 7).date()
        assert DateValidator.is_business_day(sunday) is False
    
    def test_get_date_range(self):
        """Test getting date range"""
        from_date = datetime(2024, 1, 1).date()
        to_date = datetime(2024, 1, 5).date()
        
        dates = DateValidator.get_date_range(from_date, to_date)
        
        assert len(dates) == 5
        assert dates[0] == from_date
        assert dates[-1] == to_date
    
    def test_format_date_for_spark(self):
        """Test date formatting for Spark SQL"""
        date = datetime(2024, 1, 15).date()
        formatted = DateValidator.format_date_for_spark(date)
        
        assert formatted == '2024-01-15'
    
    def test_format_date_for_display(self):
        """Test date formatting for display"""
        date = datetime(2024, 1, 15).date()
        formatted = DateValidator.format_date_for_display(date)
        
        assert formatted == '2024-01-15'
    
    def test_parse_abap_date(self):
        """Test parsing ABAP DATS format"""
        abap_date = '20240115'
        parsed = DateValidator.parse_abap_date(abap_date)
        
        assert parsed == datetime(2024, 1, 15).date()
    
    def test_to_abap_date(self):
        """Test converting to ABAP DATS format"""
        date = datetime(2024, 1, 15).date()
        abap_format = DateValidator.to_abap_date(date)
        
        assert abap_format == '20240115'
    
    def test_abap_date_round_trip(self):
        """Test round-trip conversion ABAP <-> Python"""
        original_date = datetime(2024, 1, 15).date()
        abap_format = DateValidator.to_abap_date(original_date)
        parsed_back = DateValidator.parse_abap_date(abap_format)
        
        assert original_date == parsed_back


class TestParameterValidation:
    """Integration tests for parameter validation scenarios"""
    
    @patch('sys.argv', ['test', '--from-date', '2024-01-01', '--to-date', '2024-01-07'])
    def test_valid_week_range(self):
        """Test valid one-week date range"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        assert params['from_date_parsed'] == datetime(2024, 1, 1).date()
        assert params['to_date_parsed'] == datetime(2024, 1, 7).date()
    
    @patch('sys.argv', ['test', '--from-date', '2024-01-01', '--to-date', '2024-12-31'])
    def test_valid_year_range(self):
        """Test valid one-year date range"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        date_diff = (params['to_date_parsed'] - params['from_date_parsed']).days
        assert date_diff == 365
    
    @patch('sys.argv', ['test'])
    def test_default_date_range(self):
        """Test default date range (last 7 days)"""
        handler = ParameterHandler()
        params = handler.parse_arguments()
        
        expected_from = datetime.now().date() - timedelta(days=7)
        expected_to = datetime.now().date()
        
        assert params['from_date_parsed'] == expected_from
        assert params['to_date_parsed'] == expected_to


if __name__ == '__main__':
    pytest.main([__file__, '-v'])