*&---------------------------------------------------------------------*
*& Type Pool: ZETL_TYPES
*& Description: Common type definitions for ETL system
*&---------------------------------------------------------------------*

CLASS zetl_types DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    " Raw sales data structure
    TYPES: BEGIN OF ty_raw_sales,
             trans_id    TYPE char10,
             trans_date  TYPE dats,
             customer_id TYPE char10,
             product_id  TYPE char10,
             quantity    TYPE i,
             unit_price  TYPE p LENGTH 16 DECIMALS 2,
             currency    TYPE waers,
             sales_rep   TYPE char20,
             region      TYPE char10,
             status      TYPE char1,
             created_at  TYPE timestampl,
             created_by  TYPE syuname,
           END OF ty_raw_sales,
           tt_raw_sales TYPE STANDARD TABLE OF ty_raw_sales WITH DEFAULT KEY.

    " Analytics data structure
    TYPES: BEGIN OF ty_analytics,
             analytics_id    TYPE char20,
             trans_date      TYPE dats,
             customer_id     TYPE char10,
             product_id      TYPE char10,
             total_quantity  TYPE i,
             gross_amount    TYPE p LENGTH 16 DECIMALS 2,
             net_amount      TYPE p LENGTH 16 DECIMALS 2,
             discount_amount TYPE p LENGTH 16 DECIMALS 2,
             tax_amount      TYPE p LENGTH 16 DECIMALS 2,
             currency        TYPE waers,
             sales_rep       TYPE char20,
             region          TYPE char10,
             profit_margin   TYPE p LENGTH 5 DECIMALS 2,
             category        TYPE char10,
             etl_run_id      TYPE char20,
             loaded_at       TYPE timestampl,
             loaded_by       TYPE syuname,
           END OF ty_analytics,
           tt_analytics TYPE STANDARD TABLE OF ty_analytics WITH DEFAULT KEY.

    " ETL log structure
    TYPES: BEGIN OF ty_etl_log,
             log_id            TYPE char20,
             etl_run_id        TYPE char20,
             execution_date    TYPE dats,
             execution_time    TYPE tims,
             process_step      TYPE char20,
             status            TYPE char1,
             records_processed TYPE i,
             records_success   TYPE i,
             records_error     TYPE i,
             message           TYPE char255,
             created_at        TYPE timestampl,
             created_by        TYPE syuname,
           END OF ty_etl_log,
           tt_etl_log TYPE STANDARD TABLE OF ty_etl_log WITH DEFAULT KEY.

    " ETL configuration structure
    TYPES: BEGIN OF ty_etl_config,
             batch_size      TYPE i,
             commit_interval TYPE i,
             parallel_jobs   TYPE i,
             retry_attempts  TYPE i,
             timeout_seconds TYPE i,
           END OF ty_etl_config.

    " ETL statistics structure
    TYPES: BEGIN OF ty_etl_statistics,
             total_records    TYPE i,
             success_records  TYPE i,
             error_records    TYPE i,
             warning_records  TYPE i,
             start_time       TYPE timestampl,
             end_time         TYPE timestampl,
             duration_seconds TYPE i,
           END OF ty_etl_statistics.

    " Status codes
    TYPES: BEGIN OF ty_status_codes,
             new       TYPE char1,
             processed TYPE char1,
             error     TYPE char1,
             warning   TYPE char1,
           END OF ty_status_codes.

ENDCLASS.

CLASS zetl_types IMPLEMENTATION.
  " No implementation needed for type pool
ENDCLASS.
