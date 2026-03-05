*&---------------------------------------------------------------------*
*& Class: ZCL_ETL_TRANSFORMER
*& Description: Transforms raw sales data into analytics format
*&---------------------------------------------------------------------*

CLASS zcl_etl_transformer DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES: BEGIN OF ty_analytics,
             analytics_id    TYPE char20,
             trans_date      TYPE dats,
             customer_id     TYPE char10,
             product_id      TYPE char10,
             total_quantity  TYPE i,
             gross_amount    TYPE p LENGTH 16 DECIMALS 2,
             net_amount      TYPE p LENGTH 16 DECIMALS 2,
             discount_amount TYPE p LENGTH 16 DECIMALS 2,
             tax_amount      TYPE p LENGTH 16 DECIMALS 2,
             currency        TYPE waers,
             sales_rep       TYPE char20,
             region          TYPE char10,
             profit_margin   TYPE p LENGTH 5 DECIMALS 2,
             category        TYPE char10,
             etl_run_id      TYPE char20,
           END OF ty_analytics.

    TYPES: tt_analytics TYPE STANDARD TABLE OF ty_analytics WITH DEFAULT KEY.

    METHODS constructor
      IMPORTING
        io_logger TYPE REF TO zcl_etl_logger.

    METHODS transform_data
      IMPORTING
        it_raw_data        TYPE zcl_etl_extractor=>tt_raw_sales
      EXPORTING
        et_analytics_data  TYPE tt_analytics
      RETURNING
        VALUE(rv_success)  TYPE abap_bool.

  PRIVATE SECTION.
    DATA: mo_logger TYPE REF TO zcl_etl_logger.

    METHODS calculate_analytics
      IMPORTING
        is_raw_data            TYPE zcl_etl_extractor=>ty_raw_sales
      RETURNING
        VALUE(rs_analytics)    TYPE ty_analytics.

    METHODS categorize_sale
      IMPORTING
        iv_gross_amount        TYPE p
      RETURNING
        VALUE(rv_category)     TYPE char10.

ENDCLASS.

CLASS zcl_etl_transformer IMPLEMENTATION.

  METHOD constructor.
    mo_logger = io_logger.
  ENDMETHOD.

  METHOD transform_data.
    DATA: lv_count TYPE i,
          lv_success_count TYPE i,
          lv_error_count TYPE i.

    TRY.
        mo_logger->log_message(
          iv_step    = 'TRANSFORM'
          iv_status  = 'S'
          iv_message = 'Starting data transformation' ).

        CLEAR: et_analytics_data, lv_success_count, lv_error_count.

        LOOP AT it_raw_data INTO DATA(ls_raw).
          TRY.
              " Transform each record
              DATA(ls_analytics) = calculate_analytics( ls_raw ).
              APPEND ls_analytics TO et_analytics_data.
              ADD 1 TO lv_success_count.

            CATCH cx_root INTO DATA(lx_transform_error).
              ADD 1 TO lv_error_count.
              mo_logger->log_message(
                iv_step    = 'TRANSFORM'
                iv_status  = 'W'
                iv_message = |Record {ls_raw-trans_id} transformation failed| ).
          ENDTRY.
        ENDLOOP.

        lv_count = lines( it_raw_data ).

        mo_logger->log_message(
          iv_step              = 'TRANSFORM'
          iv_status            = 'S'
          iv_records_processed = lv_count
          iv_records_success   = lv_success_count
          iv_records_error     = lv_error_count
          iv_message           = |Transformed {lv_success_count} of {lv_count} records| ).

        rv_success = abap_true.

      CATCH cx_root INTO DATA(lx_error).
        mo_logger->log_message(
          iv_step    = 'TRANSFORM'
          iv_status  = 'E'
          iv_message = |Transformation failed: {lx_error->get_text( )}| ).

        rv_success = abap_false.
    ENDTRY.

  ENDMETHOD.

  METHOD calculate_analytics.
    DATA: lv_gross TYPE p LENGTH 16 DECIMALS 2,
          lv_discount TYPE p LENGTH 16 DECIMALS 2,
          lv_tax TYPE p LENGTH 16 DECIMALS 2,
          lv_net TYPE p LENGTH 16 DECIMALS 2,
          lv_cost TYPE p LENGTH 16 DECIMALS 2,
          lv_profit_margin TYPE p LENGTH 5 DECIMALS 2.

    " Calculate amounts
    lv_gross = is_raw_data-quantity * is_raw_data-unit_price.

    " Calculate discount (5% for quantities > 10, 10% for > 15)
    IF is_raw_data-quantity > 15.
      lv_discount = lv_gross * '0.10'.
    ELSEIF is_raw_data-quantity > 10.
      lv_discount = lv_gross * '0.05'.
    ELSE.
      lv_discount = 0.
    ENDIF.

    " Calculate tax (8% on gross - discount)
    lv_tax = ( lv_gross - lv_discount ) * '0.08'.

    " Calculate net amount
    lv_net = lv_gross - lv_discount + lv_tax.

    " Calculate profit margin (simplified: assume cost is 60% of unit price)
    lv_cost = is_raw_data-quantity * is_raw_data-unit_price * '0.60'.
    lv_profit_margin = ( ( lv_net - lv_cost ) / lv_net ) * 100.

    " Generate analytics ID
    DATA(lv_timestamp) = |{ sy-datum }{ sy-uzeit }|.
    CONCATENATE 'ANL' is_raw_data-trans_id lv_timestamp+8(6) INTO DATA(lv_analytics_id).

    " Build analytics record
    rs_analytics = VALUE #(
      analytics_id    = lv_analytics_id
      trans_date      = is_raw_data-trans_date
      customer_id     = is_raw_data-customer_id
      product_id      = is_raw_data-product_id
      total_quantity  = is_raw_data-quantity
      gross_amount    = lv_gross
      net_amount      = lv_net
      discount_amount = lv_discount
      tax_amount      = lv_tax
      currency        = is_raw_data-currency
      sales_rep       = is_raw_data-sales_rep
      region          = is_raw_data-region
      profit_margin   = lv_profit_margin
      category        = categorize_sale( lv_gross )
      etl_run_id      = mo_logger->get_etl_run_id( )
    ).

  ENDMETHOD.

  METHOD categorize_sale.
    " Categorize based on gross amount
    IF iv_gross_amount >= 2000.
      rv_category = 'HIGH'.
    ELSEIF iv_gross_amount >= 500.
      rv_category = 'MEDIUM'.
    ELSE.
      rv_category = 'LOW'.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
