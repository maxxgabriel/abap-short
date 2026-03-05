*&---------------------------------------------------------------------*
*& Class: ZCL_ETL_LOGGER
*& Description: Utility class for ETL logging
*&---------------------------------------------------------------------*

CLASS zcl_etl_logger DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES: BEGIN OF ty_log_entry,
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
           END OF ty_log_entry.

    METHODS constructor
      IMPORTING
        iv_etl_run_id TYPE char20.

    METHODS log_message
      IMPORTING
        iv_step              TYPE char20
        iv_status            TYPE char1
        iv_records_processed TYPE i DEFAULT 0
        iv_records_success   TYPE i DEFAULT 0
        iv_records_error     TYPE i DEFAULT 0
        iv_message           TYPE char255.

    METHODS get_etl_run_id
      RETURNING VALUE(rv_run_id) TYPE char20.

  PRIVATE SECTION.
    DATA: mv_etl_run_id TYPE char20.

    METHODS generate_log_id
      RETURNING VALUE(rv_log_id) TYPE char20.

ENDCLASS.

CLASS zcl_etl_logger IMPLEMENTATION.

  METHOD constructor.
    mv_etl_run_id = iv_etl_run_id.
  ENDMETHOD.

  METHOD log_message.
    DATA: ls_log TYPE ty_log_entry,
          lv_timestamp TYPE timestampl.

    " Generate log entry
    ls_log-log_id            = generate_log_id( ).
    ls_log-etl_run_id        = mv_etl_run_id.
    ls_log-execution_date    = sy-datum.
    ls_log-execution_time    = sy-uzeit.
    ls_log-process_step      = iv_step.
    ls_log-status            = iv_status.
    ls_log-records_processed = iv_records_processed.
    ls_log-records_success   = iv_records_success.
    ls_log-records_error     = iv_records_error.
    ls_log-message           = iv_message.

    " Insert into database (simplified - actual implementation would use INSERT)
    " INSERT zetl_log FROM @( VALUE #(
    "   client = sy-mandt
    "   log_id = ls_log-log_id
    "   ... rest of fields
    " ) ).

    " Console output for demonstration
    WRITE: / ls_log-execution_time, ls_log-process_step, ls_log-status, ls_log-message.

  ENDMETHOD.

  METHOD generate_log_id.
    DATA: lv_timestamp TYPE string.

    " Generate unique log ID based on timestamp
    GET TIME STAMP FIELD DATA(lv_ts).
    lv_timestamp = lv_ts.
    CONCATENATE 'LOG' lv_timestamp+0(14) INTO rv_log_id.

  ENDMETHOD.

  METHOD get_etl_run_id.
    rv_run_id = mv_etl_run_id.
  ENDMETHOD.

ENDCLASS.
