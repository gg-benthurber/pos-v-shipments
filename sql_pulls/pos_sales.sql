WITH items AS (
    SELECT DISTINCT 
        ITEM_NUMBER,
        PRODUCTLINE,
        PRODUCTTYPE,
        PRODUCTVARIANT,
        PRODUCTFORM
    FROM POS_TEST_DB.SCH_ORACLE.ORACLE_ITEM
    WHERE 
        UPPER(BRAND) = 'OKEEFFES'
        AND ENTITY = 'The Gorilla Glue Company LLC'
        AND ORGANIZATION_NAME = 'Gorilla Glue Tri-County'
),
base_data AS (
    SELECT 
        iw."Customer" AS customer,
        i.PRODUCTLINE,
        i.PRODUCTTYPE,
        i.PRODUCTVARIANT,
        i.PRODUCTFORM,
        "Updated Week Ending Date",
        SUM(iw."POS Units") AS units,
        SUM(iw."POS Dollars") AS sales
    FROM POS_TEST_DB.SCH_SUMMARY.POS_ITEMWEEKLY_XCHANGE_VIEW iw
    INNER JOIN items i
        ON i.ITEM_NUMBER = iw."Part Number"
    WHERE 
        iw."Updated Week Ending Date" BETWEEN DATE '2024-04-21' AND DATE '2025-04-20'
    GROUP BY 
        iw."Customer",
        i.PRODUCTLINE,
        i.PRODUCTTYPE,
        i.PRODUCTVARIANT,
        i.PRODUCTFORM,
        "Updated Week Ending Date"
),
customer_totals AS (
    SELECT 
        customer,
        SUM(sales) AS total_sales_by_customer
    FROM base_data
    GROUP BY customer
),
ranked_customers AS (
    SELECT 
        customer,
        total_sales_by_customer,
        RANK() OVER (ORDER BY total_sales_by_customer DESC) AS customer_rank
    FROM customer_totals
    WHERE total_sales_by_customer IS NOT NULL
)
SELECT 
    bd.customer,
    bd."Updated Week Ending Date",
    bd.PRODUCTLINE,
    bd.PRODUCTTYPE,
    bd.PRODUCTVARIANT,
    bd.PRODUCTFORM,
    bd.units,
    bd.sales,
    ct.total_sales_by_customer,
    rc.customer_rank
FROM base_data bd
JOIN customer_totals ct
    ON bd.customer = ct.customer
JOIN ranked_customers rc
    ON bd.customer = rc.customer;
