# Sales ETL System - ABAP Project

## Overview
A moderate-complexity ABAP ETL (Extract, Transform, Load) system that processes sales transaction data. The system extracts raw sales data, applies business transformations and analytics calculations, and loads the results into a target analytics table with comprehensive logging.

## Project Structure

```
abap-short/
├── Database Tables/
│   ├── ZSALES_RAW.txt          # Source: Raw sales transactions
│   ├── ZSALES_ANALYTICS.txt    # Target: Transformed analytics data
│   └── ZETL_LOG.txt            # ETL execution logs
│
├── ABAP Classes/
│   ├── ZCL_ETL_LOGGER.abap       # Logging utility
│   ├── ZCL_ETL_EXTRACTOR.abap    # Data extraction component
│   ├── ZCL_ETL_TRANSFORMER.abap  # Data transformation component
│   ├── ZCL_ETL_LOADER.abap       # Data loading component
│   └── ZCL_ETL_ORCHESTRATOR.abap # Main ETL orchestrator
│
└── Executable Programs/
    └── Z_SALES_ETL_MAIN.abap     # Main ETL report
```

## Architecture

### ETL Flow
1. **Extract** → Reads raw sales data from ZSALES_RAW table
2. **Transform** → Applies business logic and calculations
3. **Load** → Writes transformed data to ZSALES_ANALYTICS table

### Components

#### 1. Database Tables

**ZSALES_RAW** (Source Table)
- Stores raw sales transactions
- Fields: transaction ID, date, customer, product, quantity, price, status
- Status: 'N' (New), 'P' (Processed), 'E' (Error)

**ZSALES_ANALYTICS** (Target Table)
- Stores transformed analytics data
- Includes: gross/net amounts, discounts, taxes, profit margins
- Categorizes sales: HIGH (≥$2000), MEDIUM (≥$500), LOW (<$500)

**ZETL_LOG** (Log Table)
- Tracks ETL execution details
- Records: run ID, process step, status, record counts, messages

#### 2. ABAP Classes

**ZCL_ETL_LOGGER**
- Centralized logging utility
- Generates unique log IDs
- Tracks execution at each ETL step

**ZCL_ETL_EXTRACTOR**
- Extracts data from source table
- Filters by date range
- Handles extraction errors

**ZCL_ETL_TRANSFORMER**
- Applies business transformations
- Calculates: discounts, taxes, profit margins
- Categorizes sales by amount
- Discount rules:
  - 10% for quantity > 15
  - 5% for quantity > 10
  - 0% otherwise
- Tax: 8% on (gross - discount)
- Profit margin: Assumes 60% cost ratio

**ZCL_ETL_LOADER**
- Validates transformed data
- Loads into target table
- Updates source record status
- Error handling for failed loads

**ZCL_ETL_ORCHESTRATOR**
- Coordinates entire ETL process
- Manages component lifecycle
- Generates unique ETL run IDs
- Tracks execution timing
- Provides summary reports

#### 3. Main Report

**Z_SALES_ETL_MAIN**
- Executable program with selection screen
- Parameters:
  - From Date / To Date (date range)
  - Test Mode (checkbox)
- Displays execution progress
- Shows summary statistics

## Key Features

### Data Transformation Logic
- **Discount Calculation**: Volume-based discounts
- **Tax Calculation**: 8% sales tax
- **Profit Margin**: Based on cost ratio
- **Categorization**: Automatic sale categorization
- **Data Enrichment**: Adds ETL metadata

### Error Handling
- Try-catch blocks at each step
- Record-level error tracking
- Graceful degradation
- Detailed error logging

### Logging & Monitoring
- Unique ETL run IDs
- Step-by-step execution tracking
- Success/error counts per step
- Timestamp tracking
- Summary reports

### Validation
- Required field validation
- Business rule validation
- Data type validation
- Currency validation

## Usage

### Setup (in SAP System)
1. Create database tables using SE11:
   - ZSALES_RAW
   - ZSALES_ANALYTICS
   - ZETL_LOG

2. Create ABAP classes using SE24:
   - ZCL_ETL_LOGGER
   - ZCL_ETL_EXTRACTOR
   - ZCL_ETL_TRANSFORMER
   - ZCL_ETL_LOADER
   - ZCL_ETL_ORCHESTRATOR

3. Create report program using SE38:
   - Z_SALES_ETL_MAIN

### Running the ETL

**Via Transaction SE38/SA38:**
```
Execute: Z_SALES_ETL_MAIN
```

**Selection Screen:**
- From Date: Start of date range (default: 7 days ago)
- To Date: End of date range (default: today)
- Test Mode: Enable to prevent database commits

**Output:**
- ETL Run ID
- Processing phases (Extract, Transform, Load)
- Record counts per phase
- Success/error statistics
- Execution summary with duration

### Sample Data
The extractor includes sample data for demonstration:
- 5 sample transactions
- Various products, customers, regions
- Different quantities for discount testing

## Technical Details

### Performance Considerations
- Batch processing capability
- Date range filtering
- Status-based filtering (only 'N' records)
- Efficient internal table operations

### Extensibility Points
- Add more transformation rules in ZCL_ETL_TRANSFORMER
- Implement different data sources in ZCL_ETL_EXTRACTOR
- Add validation rules in ZCL_ETL_LOADER
- Extend logging in ZCL_ETL_LOGGER

### Best Practices Implemented
- Separation of concerns (Extract/Transform/Load)
- Object-oriented design
- Dependency injection (logger)
- Error handling at all levels
- Comprehensive logging
- Data validation
- Test mode support

## Complexity Level: Simple-Moderate

**Simple Aspects:**
- Clear ETL pattern
- Straightforward transformations
- Basic business logic

**Moderate Aspects:**
- Multiple coordinated classes
- Comprehensive error handling
- Logging framework
- Data validation
- Status management
- Multiple transformation rules

## Future Enhancements
- Parallel processing for large datasets
- Delta load capability
- Data quality checks
- Email notifications
- Performance metrics
- Scheduling integration
- Error recovery mechanisms
- Audit trail

## Notes
- This is a demonstration project with sample data
- In production, uncomment database operations
- Adjust transformation logic per business requirements
- Configure logging table retention policy
- Consider archiving old ETL logs
