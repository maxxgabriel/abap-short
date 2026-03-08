"""
Main ETL execution module
Integrates parameter handler with ETL orchestration
"""
import sys
from pathlib import Path
from typing import Optional

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from parameter_handler import ParameterHandler, DateValidator


def main():
    """Main ETL execution function"""
    try:
        # Initialize parameter handler
        print("Initializing ETL parameter handler...")
        param_handler = ParameterHandler()
        
        # Parse and validate parameters
        print("Parsing command-line parameters...")
        params = param_handler.parse_arguments()
        
        # Display parameters
        print("\n")
        print(param_handler.display_parameters())
        print("\n")
        
        # Additional date validations
        from_date = params['from_date_parsed']
        to_date = params['to_date_parsed']
        
        # Get business days in range
        date_range = DateValidator.get_date_range(from_date, to_date)
        business_days = [d for d in date_range if DateValidator.is_business_day(d)]
        
        print(f"Total days in range: {len(date_range)}")
        print(f"Business days in range: {len(business_days)}")
        print(f"Weekend days: {len(date_range) - len(business_days)}")
        print("\n")
        
        # Display date range details
        print("Date Range Details:")
        print(f"  From Date (Spark format): {DateValidator.format_date_for_spark(from_date)}")
        print(f"  To Date (Spark format): {DateValidator.format_date_for_spark(to_date)}")
        print(f"  From Date (ABAP format): {DateValidator.to_abap_date(from_date)}")
        print(f"  To Date (ABAP format): {DateValidator.to_abap_date(to_date)}")
        print("\n")
        
        # Here you would integrate with the actual ETL orchestrator
        # from orchestrator import ETLOrchestrator
        # orchestrator = ETLOrchestrator(params)
        # success = orchestrator.run_etl()
        
        print("Parameter validation successful!")
        print("Ready to execute ETL process...")
        
        if params['test_mode']:
            print("\n*** TEST MODE - No data will be committed ***")
        
        return 0
        
    except ValueError as e:
        print(f"\n*** PARAMETER VALIDATION ERROR ***", file=sys.stderr)
        print(f"Error: {str(e)}", file=sys.stderr)
        return 1
        
    except Exception as e:
        print(f"\n*** FATAL ERROR ***", file=sys.stderr)
        print(f"Error: {str(e)}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return 2


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)