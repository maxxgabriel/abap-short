*&---------------------------------------------------------------------*
*& Class: ZCL_ETL_ORCHESTRATOR
*& Description: Main ETL orchestrator that coordinates the ETL process
*&---------------------------------------------------------------------*

CLASS zcl_etl_orchestrator DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS constructor.

    METHODS run_etl
      IMPORTING
        iv_from_date      TYPE dats
        iv_to_date        TYPE dats
      RETURNING
        VALUE(rv_success) TYPE abap_bool.

    METHODS get_etl_run_id
      RETURNING
        VALUE(rv_run_id) TYPE char20.

    METHODS display_summary.

  PRIVATE SECTION.
    DATA: mo_logger      TYPE REF TO zcl_etl_logger,
          mo_extractor   TYPE REF TO zcl_etl_extractor,
          mo_transformer TYPE REF TO zcl_etl_transformer,
          mo_loader      TYPE REF TO zcl_etl_loader,
          mv_etl_run_id  TYPE char20,
          mv_start_time  TYPE timestampl,
          mv_end_time    TYPE timestampl.

    METHODS generate_etl_run_id
      RETURNING
        VALUE(rv_run_id) TYPE char20.

ENDCLASS.

CLASS zcl_etl_orchestrator IMPLEMENTATION.

  METHOD constructor.
    " Generate unique ETL run ID
    mv_etl_run_id = generate_etl_run_id( ).

    " Initialize logger
    mo_logger = NEW zcl_etl_logger( mv_etl_run_id ).

    " Initialize ETL components
    mo_extractor   = NEW zcl_etl_extractor( mo_logger ).
    mo_transformer = NEW zcl_etl_transformer( mo_logger ).
    mo_loader      = NEW zcl_etl_loader( mo_logger ).

    " Log initialization
    mo_logger->log_message(
      iv_step    = 'INIT'
      iv_status  = 'S'
      iv_message = |ETL process initialized with run ID: {mv_etl_run_id}| ).

  ENDMETHOD.

  METHOD run_etl.
    DATA: lt_raw_data       TYPE zcl_etl_extractor=>tt_raw_sales,
          lt_analytics_data TYPE zcl_etl_transformer=>tt_analytics,
          lv_extract_success TYPE abap_bool,
          lv_transform_success TYPE abap_bool,
          lv_load_success TYPE abap_bool.

    TRY.
        " Capture start time
        GET TIME STAMP FIELD mv_start_time.

        mo_logger->log_message(
          iv_step    = 'START'
          iv_status  = 'S'
          iv_message = |ETL process started at {mv_start_time}| ).

        " Step 1: Extract
        WRITE: / |=== EXTRACT Phase ===|.
        lv_extract_success = mo_extractor->extract_data(
          EXPORTING
            iv_from_date  = iv_from_date
            iv_to_date    = iv_to_date
          IMPORTING
            et_sales_data = lt_raw_data ).

        IF lv_extract_success = abap_false.
          RAISE EXCEPTION TYPE cx_sy_arithmetic_error
            MESSAGE TEXT-001. "Extraction failed
        ENDIF.

        " Step 2: Transform
        WRITE: / |=== TRANSFORM Phase ===|.
        lv_transform_success = mo_transformer->transform_data(
          EXPORTING
            it_raw_data       = lt_raw_data
          IMPORTING
            et_analytics_data = lt_analytics_data ).

        IF lv_transform_success = abap_false.
          RAISE EXCEPTION TYPE cx_sy_arithmetic_error
            MESSAGE TEXT-002. "Transformation failed
        ENDIF.

        " Step 3: Load
        WRITE: / |=== LOAD Phase ===|.
        lv_load_success = mo_loader->load_data( lt_analytics_data ).

        IF lv_load_success = abap_false.
          RAISE EXCEPTION TYPE cx_sy_arithmetic_error
            MESSAGE TEXT-003. "Load failed
        ENDIF.

        " Capture end time
        GET TIME STAMP FIELD mv_end_time.

        mo_logger->log_message(
          iv_step    = 'COMPLETE'
          iv_status  = 'S'
          iv_message = |ETL process completed successfully at {mv_end_time}| ).

        rv_success = abap_true.

      CATCH cx_root INTO DATA(lx_error).
        GET TIME STAMP FIELD mv_end_time.

        mo_logger->log_message(
          iv_step    = 'ERROR'
          iv_status  = 'E'
          iv_message = |ETL process failed: {lx_error->get_text( )}| ).

        rv_success = abap_false.
    ENDTRY.

  ENDMETHOD.

  METHOD generate_etl_run_id.
    DATA: lv_timestamp TYPE string.

    GET TIME STAMP FIELD DATA(lv_ts).
    lv_timestamp = lv_ts.
    CONCATENATE 'ETL' lv_timestamp+0(14) INTO rv_run_id.

  ENDMETHOD.

  METHOD get_etl_run_id.
    rv_run_id = mv_etl_run_id.
  ENDMETHOD.

  METHOD display_summary.
    DATA: lv_duration TYPE i.

    WRITE: / |{'=' WIDTH = 60 }|,
           / |ETL Process Summary|,
           / |{'=' WIDTH = 60 }|,
           / |ETL Run ID:    {mv_etl_run_id}|,
           / |Start Time:    {mv_start_time}|,
           / |End Time:      {mv_end_time}|.

    " Calculate duration (simplified)
    IF mv_end_time > mv_start_time.
      lv_duration = mv_end_time - mv_start_time.
      WRITE: / |Duration:       {lv_duration} seconds|.
    ENDIF.

    WRITE: / |{'=' WIDTH = 60 }|.

  ENDMETHOD.

ENDCLASS.
