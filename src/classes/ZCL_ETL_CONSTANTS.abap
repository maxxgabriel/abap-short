*&---------------------------------------------------------------------*
*& Class: ZCL_ETL_CONSTANTS
*& Description: Constants and configuration for ETL system
*&---------------------------------------------------------------------*

CLASS zcl_etl_constants DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    " Status codes
    CONSTANTS:
      BEGIN OF gc_status,
        new       TYPE char1 VALUE 'N',
        processed TYPE char1 VALUE 'P',
        error     TYPE char1 VALUE 'E',
        warning   TYPE char1 VALUE 'W',
        success   TYPE char1 VALUE 'S',
        info      TYPE char1 VALUE 'I',
      END OF gc_status.

    " ETL process steps
    CONSTANTS:
      BEGIN OF gc_step,
        init      TYPE char20 VALUE 'INIT',
        extract   TYPE char20 VALUE 'EXTRACT',
        transform TYPE char20 VALUE 'TRANSFORM',
        load      TYPE char20 VALUE 'LOAD',
        validate  TYPE char20 VALUE 'VALIDATE',
        complete  TYPE char20 VALUE 'COMPLETE',
        error     TYPE char20 VALUE 'ERROR',
      END OF gc_step.

    " Sale categories
    CONSTANTS:
      BEGIN OF gc_category,
        high   TYPE char10 VALUE 'HIGH',
        medium TYPE char10 VALUE 'MEDIUM',
        low    TYPE char10 VALUE 'LOW',
      END OF gc_category.

    " Business rules - Discount thresholds
    CONSTANTS:
      gc_discount_qty_tier1 TYPE i VALUE 10,
      gc_discount_qty_tier2 TYPE i VALUE 15,
      gc_discount_rate_tier1 TYPE p LENGTH 3 DECIMALS 2 VALUE '0.05',
      gc_discount_rate_tier2 TYPE p LENGTH 3 DECIMALS 2 VALUE '0.10'.

    " Business rules - Tax rate
    CONSTANTS:
      gc_tax_rate TYPE p LENGTH 3 DECIMALS 2 VALUE '0.08'.

    " Business rules - Cost ratio
    CONSTANTS:
      gc_cost_ratio TYPE p LENGTH 3 DECIMALS 2 VALUE '0.60'.

    " Business rules - Category thresholds
    CONSTANTS:
      gc_category_high_threshold TYPE p LENGTH 16 DECIMALS 2 VALUE '2000.00',
      gc_category_medium_threshold TYPE p LENGTH 16 DECIMALS 2 VALUE '500.00'.

    " ETL configuration defaults
    CONSTANTS:
      gc_default_batch_size TYPE i VALUE 1000,
      gc_default_commit_interval TYPE i VALUE 500,
      gc_default_retry_attempts TYPE i VALUE 3,
      gc_default_timeout_seconds TYPE i VALUE 3600.

    " ID prefixes
    CONSTANTS:
      gc_prefix_etl_run TYPE char3 VALUE 'ETL',
      gc_prefix_log_id TYPE char3 VALUE 'LOG',
      gc_prefix_analytics_id TYPE char3 VALUE 'ANL'.

    " Message texts
    CONSTANTS:
      gc_msg_init_success TYPE string VALUE 'ETL process initialized successfully',
      gc_msg_extract_start TYPE string VALUE 'Starting data extraction',
      gc_msg_extract_complete TYPE string VALUE 'Data extraction completed',
      gc_msg_transform_start TYPE string VALUE 'Starting data transformation',
      gc_msg_transform_complete TYPE string VALUE 'Data transformation completed',
      gc_msg_load_start TYPE string VALUE 'Starting data load',
      gc_msg_load_complete TYPE string VALUE 'Data load completed',
      gc_msg_etl_complete TYPE string VALUE 'ETL process completed successfully',
      gc_msg_etl_error TYPE string VALUE 'ETL process failed'.

ENDCLASS.

CLASS zcl_etl_constants IMPLEMENTATION.
  " No implementation needed for constants class
ENDCLASS.
