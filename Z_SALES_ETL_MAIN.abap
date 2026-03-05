*&---------------------------------------------------------------------*
*& Report: Z_SALES_ETL_MAIN
*& Description: Main executable program for Sales ETL process
*&---------------------------------------------------------------------*

REPORT z_sales_etl_main.

*----------------------------------------------------------------------*
* Data Declarations
*----------------------------------------------------------------------*
DATA: go_orchestrator TYPE REF TO zcl_etl_orchestrator,
      gv_from_date    TYPE dats,
      gv_to_date      TYPE dats,
      gv_success      TYPE abap_bool.

*----------------------------------------------------------------------*
* Selection Screen
*----------------------------------------------------------------------*
SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.

  PARAMETERS: p_fdate TYPE dats DEFAULT sy-datum OBLIGATORY,
              p_tdate TYPE dats DEFAULT sy-datum OBLIGATORY.

  SELECTION-SCREEN SKIP.

  PARAMETERS: p_test TYPE abap_bool AS CHECKBOX DEFAULT abap_true.

SELECTION-SCREEN END OF BLOCK b1.

*----------------------------------------------------------------------*
* Initialization
*----------------------------------------------------------------------*
INITIALIZATION.
  " Set default dates (last 7 days)
  p_fdate = sy-datum - 7.
  p_tdate = sy-datum.

*----------------------------------------------------------------------*
* At Selection Screen - Validation
*----------------------------------------------------------------------*
AT SELECTION-SCREEN.
  IF p_fdate > p_tdate.
    MESSAGE 'From Date cannot be later than To Date' TYPE 'E'.
  ENDIF.

  IF p_tdate > sy-datum.
    MESSAGE 'To Date cannot be in the future' TYPE 'E'.
  ENDIF.

*----------------------------------------------------------------------*
* Main Processing
*----------------------------------------------------------------------*
START-OF-SELECTION.

  " Display header
  WRITE: / |{'*' WIDTH = 70 }|,
         / |*{ ' ' WIDTH = 68 }*|,
         / |*{ 'Sales Data ETL Process' WIDTH = 68 ALIGN = CENTER }*|,
         / |*{ ' ' WIDTH = 68 }*|,
         / |{'*' WIDTH = 70 }|,
         /.

  " Display parameters
  WRITE: / |Processing Date Range: {p_fdate DATE = USER} to {p_tdate DATE = USER}|,
         / |Test Mode: { COND #( WHEN p_test = abap_true THEN 'Yes' ELSE 'No' ) }|,
         /.

  TRY.
      " Create orchestrator instance
      go_orchestrator = NEW zcl_etl_orchestrator( ).

      " Display ETL run ID
      WRITE: / |ETL Run ID: {go_orchestrator->get_etl_run_id( )}|, /.

      " Run ETL process
      gv_success = go_orchestrator->run_etl(
        iv_from_date = p_fdate
        iv_to_date   = p_tdate ).

      " Display results
      SKIP 2.

      IF gv_success = abap_true.
        WRITE: / |*** ETL Process Completed Successfully ***|.

        " Display summary
        SKIP 1.
        go_orchestrator->display_summary( ).

        " In test mode, don't commit
        IF p_test = abap_true.
          WRITE: / |Test mode - No data committed to database|.
        ELSE.
          COMMIT WORK.
          WRITE: / |Data committed to database|.
        ENDIF.

      ELSE.
        WRITE: / |*** ETL Process Failed ***| COLOR COL_NEGATIVE.
        WRITE: / |Please check the error logs for details.|.
      ENDIF.

    CATCH cx_root INTO DATA(lx_root).
      WRITE: / |*** Fatal Error ***| COLOR COL_NEGATIVE,
             / |Error: {lx_root->get_text( )}|.
  ENDTRY.

END-OF-SELECTION.

*----------------------------------------------------------------------*
* Text Symbols
*----------------------------------------------------------------------*
* TEXT-001: 'ETL Parameters'
*----------------------------------------------------------------------*
