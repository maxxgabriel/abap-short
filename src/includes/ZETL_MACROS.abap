*&---------------------------------------------------------------------*
*& Include: ZETL_MACROS
*& Description: Useful macros for ETL processing
*&---------------------------------------------------------------------*

" Macro to log messages with timestamp
DEFINE log_etl_message.
  mo_logger->log_message(
    iv_step    = &1
    iv_status  = &2
    iv_message = &3 ).
END-OF-DEFINITION.

" Macro to log with record counts
DEFINE log_etl_statistics.
  mo_logger->log_message(
    iv_step              = &1
    iv_status            = &2
    iv_records_processed = &3
    iv_records_success   = &4
    iv_records_error     = &5
    iv_message           = &6 ).
END-OF-DEFINITION.

" Macro to validate mandatory field
DEFINE validate_field.
  IF &1 IS INITIAL.
    rv_valid = abap_false.
    RETURN.
  ENDIF.
END-OF-DEFINITION.

" Macro to calculate percentage
DEFINE calculate_percentage.
  IF &2 > 0.
    &3 = ( &1 / &2 ) * 100.
  ELSE.
    &3 = 0.
  ENDIF.
END-OF-DEFINITION.

" Macro for error handling
DEFINE handle_etl_error.
  CATCH cx_root INTO DATA(lx_error).
    log_etl_message zcl_etl_constants=>gc_step-error
                    zcl_etl_constants=>gc_status-error
                    |&1: { lx_error->get_text( ) }|.
    rv_success = abap_false.
END-OF-DEFINITION.

" Macro to generate unique IDs
DEFINE generate_unique_id.
  GET TIME STAMP FIELD DATA(lv_timestamp_&1).
  CONCATENATE &2 lv_timestamp_&1+0(14) INTO &3.
END-OF-DEFINITION.

" Macro to format currency
DEFINE format_currency.
  WRITE &1 TO &2 CURRENCY &3.
END-OF-DEFINITION.

" Macro to add days to date
DEFINE add_days_to_date.
  CALL FUNCTION 'RP_CALC_DATE_IN_INTERVAL'
    EXPORTING
      date      = &1
      days      = &2
      months    = 0
      years     = 0
    IMPORTING
      calc_date = &3.
END-OF-DEFINITION.
