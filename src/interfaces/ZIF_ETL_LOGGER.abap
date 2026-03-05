*&---------------------------------------------------------------------*
*& Interface: ZIF_ETL_LOGGER
*& Description: Interface for ETL logging
*&---------------------------------------------------------------------*

INTERFACE zif_etl_logger
  PUBLIC.

  CONSTANTS:
    BEGIN OF gc_status,
      success TYPE char1 VALUE 'S',
      error   TYPE char1 VALUE 'E',
      warning TYPE char1 VALUE 'W',
      info    TYPE char1 VALUE 'I',
    END OF gc_status.

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

  METHODS log_message
    IMPORTING
      iv_step              TYPE char20
      iv_status            TYPE char1
      iv_records_processed TYPE i DEFAULT 0
      iv_records_success   TYPE i DEFAULT 0
      iv_records_error     TYPE i DEFAULT 0
      iv_message           TYPE char255.

  METHODS get_etl_run_id
    RETURNING
      VALUE(rv_run_id) TYPE char20.

ENDINTERFACE.
