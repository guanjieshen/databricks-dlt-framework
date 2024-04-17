CREATE
OR REFRESH STREAMING TABLE IDENTIFIER(
    concat({{ catalog }}, '.', {{ schema }},'.','orders_raw')
)
AS
SELECT
    *
FROM
    STREAM READ_FILES(
        '/databricks-datasets/retail-org/sales_orders/',
        format => "json",
        header => true
    )