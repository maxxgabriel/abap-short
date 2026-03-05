*&---------------------------------------------------------------------*
*& Include: ZETL_TOP
*& Description: Top include for common declarations
*&---------------------------------------------------------------------*

" Type definitions
INCLUDE zetl_types.

" Constants
DATA: go_constants TYPE REF TO zcl_etl_constants.

" Global variables for ETL process
DATA: gv_etl_run_id    TYPE char20,
      gv_test_mode     TYPE abap_bool,
      gv_batch_size    TYPE i,
      gv_start_time    TYPE timestampl,
      gv_end_time      TYPE timestampl.

" Global tables
DATA: gt_raw_sales     TYPE zetl_types=>tt_raw_sales,
      gt_analytics     TYPE zetl_types=>tt_analytics,
      gt_etl_log       TYPE zetl_types=>tt_etl_log.

" Global statistics
DATA: BEGIN OF gs_statistics,
        total_extracted   TYPE i,
        total_transformed TYPE i,
        total_loaded      TYPE i,
        errors_count      TYPE i,
        warnings_count    TYPE i,
      END OF gs_statistics.
