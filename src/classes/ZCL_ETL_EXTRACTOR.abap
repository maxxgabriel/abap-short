*&---------------------------------------------------------------------*
*& Class: ZCL_ETL_EXTRACTOR
*& Description: Extracts raw sales data from source
*&---------------------------------------------------------------------*

CLASS zcl_etl_extractor DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
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
           END OF ty_raw_sales.

    TYPES: tt_raw_sales TYPE STANDARD TABLE OF ty_raw_sales WITH DEFAULT KEY.

    METHODS constructor
      IMPORTING
        io_logger TYPE REF TO zcl_etl_logger.

    METHODS extract_data
      IMPORTING
        iv_from_date       TYPE dats
        iv_to_date         TYPE dats
      EXPORTING
        et_sales_data      TYPE tt_raw_sales
      RETURNING
        VALUE(rv_success)  TYPE abap_bool.

  PRIVATE SECTION.
    DATA: mo_logger TYPE REF TO zcl_etl_logger.

ENDCLASS.

CLASS zcl_etl_extractor IMPLEMENTATION.

  METHOD constructor.
    mo_logger = io_logger.
  ENDMETHOD.

  METHOD extract_data.
    DATA: lv_count TYPE i.

    TRY.
        mo_logger->log_message(
          iv_step    = 'EXTRACT'
          iv_status  = 'S'
          iv_message = |Starting extraction from {iv_from_date} to {iv_to_date}| ).

        " Simulate data extraction from ZSALES_RAW table
        " In real implementation: SELECT * FROM zsales_raw INTO TABLE @et_sales_data
        "   WHERE trans_date BETWEEN @iv_from_date AND @iv_to_date
        "   AND status = 'N'.

        " For demonstration, create sample data
        et_sales_data = VALUE #(
          ( trans_id = 'T000001' trans_date = sy-datum customer_id = 'CUST001'
            product_id = 'PROD001' quantity = 10 unit_price = '99.99'
            currency = 'USD' sales_rep = 'John Doe' region = 'NORTH' status = 'N' )
          ( trans_id = 'T000002' trans_date = sy-datum customer_id = 'CUST002'
            product_id = 'PROD002' quantity = 5 unit_price = '149.99'
            currency = 'USD' sales_rep = 'Jane Smith' region = 'SOUTH' status = 'N' )
          ( trans_id = 'T000003' trans_date = sy-datum customer_id = 'CUST003'
            product_id = 'PROD001' quantity = 20 unit_price = '99.99'
            currency = 'USD' sales_rep = 'John Doe' region = 'EAST' status = 'N' )
          ( trans_id = 'T000004' trans_date = sy-datum customer_id = 'CUST001'
            product_id = 'PROD003' quantity = 3 unit_price = '299.99'
            currency = 'USD' sales_rep = 'Bob Wilson' region = 'WEST' status = 'N' )
          ( trans_id = 'T000005' trans_date = sy-datum customer_id = 'CUST004'
            product_id = 'PROD002' quantity = 15 unit_price = '149.99'
            currency = 'USD' sales_rep = 'Jane Smith' region = 'SOUTH' status = 'N' )
        ).

        lv_count = lines( et_sales_data ).

        mo_logger->log_message(
          iv_step              = 'EXTRACT'
          iv_status            = 'S'
          iv_records_processed = lv_count
          iv_records_success   = lv_count
          iv_message           = |Extracted {lv_count} records successfully| ).

        rv_success = abap_true.

      CATCH cx_root INTO DATA(lx_error).
        mo_logger->log_message(
          iv_step    = 'EXTRACT'
          iv_status  = 'E'
          iv_message = |Extraction failed: {lx_error->get_text( )}| ).

        rv_success = abap_false.
    ENDTRY.

  ENDMETHOD.

ENDCLASS.
