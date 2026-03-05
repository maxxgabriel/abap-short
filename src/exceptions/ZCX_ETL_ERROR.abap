*&---------------------------------------------------------------------*
*& Exception Class: ZCX_ETL_ERROR
*& Description: Base exception class for ETL errors
*&---------------------------------------------------------------------*

CLASS zcx_etl_error DEFINITION
  PUBLIC
  INHERITING FROM cx_static_check
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES if_t100_message.
    INTERFACES if_t100_dyn_msg.

    CONSTANTS:
      BEGIN OF zcx_etl_error,
        msgid TYPE symsgid VALUE 'ZETL',
        msgno TYPE symsgno VALUE '001',
        attr1 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr2 TYPE scx_attrname VALUE '',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF zcx_etl_error.

    CONSTANTS:
      BEGIN OF extract_error,
        msgid TYPE symsgid VALUE 'ZETL',
        msgno TYPE symsgno VALUE '002',
        attr1 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr2 TYPE scx_attrname VALUE '',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF extract_error.

    CONSTANTS:
      BEGIN OF transform_error,
        msgid TYPE symsgid VALUE 'ZETL',
        msgno TYPE symsgno VALUE '003',
        attr1 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr2 TYPE scx_attrname VALUE '',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF transform_error.

    CONSTANTS:
      BEGIN OF load_error,
        msgid TYPE symsgid VALUE 'ZETL',
        msgno TYPE symsgno VALUE '004',
        attr1 TYPE scx_attrname VALUE 'MV_ERROR_TEXT',
        attr2 TYPE scx_attrname VALUE '',
        attr3 TYPE scx_attrname VALUE '',
        attr4 TYPE scx_attrname VALUE '',
      END OF load_error.

    DATA mv_error_text TYPE string.
    DATA mv_error_step TYPE char20.
    DATA mv_record_id TYPE char20.

    METHODS constructor
      IMPORTING
        !textid       LIKE if_t100_message=>t100key OPTIONAL
        !previous     LIKE previous OPTIONAL
        iv_error_text TYPE string OPTIONAL
        iv_error_step TYPE char20 OPTIONAL
        iv_record_id  TYPE char20 OPTIONAL.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.

CLASS zcx_etl_error IMPLEMENTATION.

  METHOD constructor ##ADT_SUPPRESS_GENERATION.
    CALL METHOD super->constructor
      EXPORTING
        previous = previous.

    me->mv_error_text = iv_error_text.
    me->mv_error_step = iv_error_step.
    me->mv_record_id = iv_record_id.

    CLEAR me->textid.
    IF textid IS INITIAL.
      if_t100_message~t100key = zcx_etl_error.
    ELSE.
      if_t100_message~t100key = textid.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
