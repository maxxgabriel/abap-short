*&---------------------------------------------------------------------*
*& Class: ZCL_ETL_LOADER
*& Description: Loads transformed data into target analytics table
*&---------------------------------------------------------------------*

CLASS zcl_etl_loader DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS constructor
      IMPORTING
        io_logger TYPE REF TO zcl_etl_logger.

    METHODS load_data
      IMPORTING
        it_analytics_data  TYPE zcl_etl_transformer=>tt_analytics
      RETURNING
        VALUE(rv_success)  TYPE abap_bool.

  PRIVATE SECTION.
    DATA: mo_logger TYPE REF TO zcl_etl_logger.

    METHODS validate_record
      IMPORTING
        is_analytics       TYPE zcl_etl_transformer=>ty_analytics
      RETURNING
        VALUE(rv_valid)    TYPE abap_bool.

ENDCLASS.

CLASS zcl_etl_loader IMPLEMENTATION.

  METHOD constructor.
    mo_logger = io_logger.
  ENDMETHOD.

  METHOD load_data.
    DATA: lv_count TYPE i,
          lv_success_count TYPE i,
          lv_error_count TYPE i.

    TRY.
        mo_logger->log_message(
          iv_step    = 'LOAD'
          iv_status  = 'S'
          iv_message = 'Starting data load' ).

        lv_count = lines( it_analytics_data ).

        LOOP AT it_analytics_data INTO DATA(ls_analytics).
          " Validate record before loading
          IF validate_record( ls_analytics ) = abap_true.
            TRY.
                " In real implementation: INSERT zsales_analytics FROM @ls_analytics.
                " For demonstration, we just count successful records
                ADD 1 TO lv_success_count.

              CATCH cx_root INTO DATA(lx_insert_error).
                ADD 1 TO lv_error_count.
                mo_logger->log_message(
                  iv_step    = 'LOAD'
                  iv_status  = 'W'
                  iv_message = |Failed to load record {ls_analytics-analytics_id}| ).
            ENDTRY.
          ELSE.
            ADD 1 TO lv_error_count.
            mo_logger->log_message(
              iv_step    = 'LOAD'
              iv_status  = 'W'
              iv_message = |Invalid record {ls_analytics-analytics_id} skipped| ).
          ENDIF.
        ENDLOOP.

        " Update status in source table
        " In real implementation: UPDATE zsales_raw SET status = 'P'
        "   WHERE trans_id IN @lt_processed_ids.

        mo_logger->log_message(
          iv_step              = 'LOAD'
          iv_status            = 'S'
          iv_records_processed = lv_count
          iv_records_success   = lv_success_count
          iv_records_error     = lv_error_count
          iv_message           = |Loaded {lv_success_count} of {lv_count} records| ).

        rv_success = abap_true.

      CATCH cx_root INTO DATA(lx_error).
        mo_logger->log_message(
          iv_step    = 'LOAD'
          iv_status  = 'E'
          iv_message = |Load failed: {lx_error->get_text( )}| ).

        rv_success = abap_false.
    ENDTRY.

  ENDMETHOD.

  METHOD validate_record.
    rv_valid = abap_true.

    " Validate required fields
    IF is_analytics-analytics_id IS INITIAL OR
       is_analytics-customer_id IS INITIAL OR
       is_analytics-product_id IS INITIAL OR
       is_analytics-gross_amount <= 0.
      rv_valid = abap_false.
    ENDIF.

    " Validate currency
    IF is_analytics-currency IS INITIAL.
      rv_valid = abap_false.
    ENDIF.

    " Validate category
    IF is_analytics-category NOT IN ('HIGH', 'MEDIUM', 'LOW').
      rv_valid = abap_false.
    ENDIF.

  ENDMETHOD.

ENDCLASS.
