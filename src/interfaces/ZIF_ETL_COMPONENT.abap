*&---------------------------------------------------------------------*
*& Interface: ZIF_ETL_COMPONENT
*& Description: Interface for all ETL components
*&---------------------------------------------------------------------*

INTERFACE zif_etl_component
  PUBLIC.

  TYPES: BEGIN OF ty_execution_result,
           success        TYPE abap_bool,
           records_total  TYPE i,
           records_success TYPE i,
           records_error  TYPE i,
           message        TYPE string,
         END OF ty_execution_result.

  METHODS execute
    RETURNING
      VALUE(rs_result) TYPE ty_execution_result
    RAISING
      zcx_etl_error.

  METHODS get_component_name
    RETURNING
      VALUE(rv_name) TYPE string.

  METHODS validate_prerequisites
    RETURNING
      VALUE(rv_valid) TYPE abap_bool.

ENDINTERFACE.
