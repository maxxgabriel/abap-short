*&---------------------------------------------------------------------*
*& Package: $ZETL
*& Description: Sales ETL System Package Definition
*&---------------------------------------------------------------------*

" Package: $ZETL
" Description: Complete ETL system for sales data processing
" Application Component: FI-AA (or your component)
" Software Component: HOME (or your component)

*&---------------------------------------------------------------------*
*& Package Structure
*&---------------------------------------------------------------------*
" This package contains:
"
" 1. Database Tables (3):
"    - ZSALES_RAW          : Source table for raw sales data
"    - ZSALES_ANALYTICS    : Target table for analytics data
"    - ZETL_LOG            : ETL execution log table
"
" 2. ABAP Classes (6):
"    - ZCL_ETL_CONSTANTS   : Constants and configuration
"    - ZCL_ETL_LOGGER      : Logging utility
"    - ZCL_ETL_EXTRACTOR   : Data extraction component
"    - ZCL_ETL_TRANSFORMER : Data transformation component
"    - ZCL_ETL_LOADER      : Data loading component
"    - ZCL_ETL_ORCHESTRATOR: ETL orchestration
"    - ZETL_TYPES          : Type definitions
"
" 3. Interfaces (2):
"    - ZIF_ETL_COMPONENT   : Component interface
"    - ZIF_ETL_LOGGER      : Logger interface
"
" 4. Exception Classes (1):
"    - ZCX_ETL_ERROR       : ETL exception class
"
" 5. Programs (1):
"    - Z_SALES_ETL_MAIN    : Main executable report
"
" 6. Includes (2):
"    - ZETL_TOP            : Common declarations
"    - ZETL_MACROS         : Utility macros
"
*&---------------------------------------------------------------------*
*& Dependencies
*&---------------------------------------------------------------------*
" External Dependencies:
" - None (self-contained package)
"
" System Requirements:
" - ABAP 7.40 or higher (for inline declarations)
" - Standard SAP Basis components
"
*&---------------------------------------------------------------------*
*& Installation Order
*&---------------------------------------------------------------------*
" 1. Create package $ZETL
" 2. Create database tables (SE11)
" 3. Create type pool (ZETL_TYPES)
" 4. Create interfaces (ZIF_*)
" 5. Create exception class (ZCX_ETL_ERROR)
" 6. Create constants class (ZCL_ETL_CONSTANTS)
" 7. Create includes (ZETL_*)
" 8. Create utility classes (ZCL_ETL_LOGGER)
" 9. Create ETL component classes (ZCL_ETL_*)
" 10. Create orchestrator (ZCL_ETL_ORCHESTRATOR)
" 11. Create main program (Z_SALES_ETL_MAIN)
"
*&---------------------------------------------------------------------*
*& Configuration
*&---------------------------------------------------------------------*
" Configuration file: config/etl_config.json
" - Adjust business rules as needed
" - Configure batch sizes and timeouts
" - Set error handling behavior
"
*&---------------------------------------------------------------------*
*& Usage
*&---------------------------------------------------------------------*
" Execute: SA38 -> Z_SALES_ETL_MAIN
" or use transaction code (if created)
"
*&---------------------------------------------------------------------*
