WITH date_bounds AS (
    SELECT 
        -- Set end_date to most recent Sunday (yesterday if today is Monday)
        DATEADD(DAY, -DAYOFWEEK(CURRENT_DATE()), CURRENT_DATE()) AS end_date,

        -- Set start_date to the Monday 52 weeks before that Sunday
        DATEADD(WEEK, -52, DATEADD(DAY, 1, DATEADD(DAY, -DAYOFWEEK(CURRENT_DATE()), CURRENT_DATE()))) AS start_date
),

items AS (
    SELECT DISTINCT 
        ITEM_NUMBER,
        PRODUCTLINE,
        PRODUCTTYPE,
        PRODUCTVARIANT,
        PRODUCTFORM
    FROM POS_TEST_DB.SCH_ORACLE.BI_ITEM
    WHERE 
        UPPER(BRAND) = 'OKEEFFES'
        AND ENTITY = 'The Gorilla Glue Company LLC'
        AND ORGANIZATION_NAME = 'Gorilla Glue Tri-County'
),
base_data AS (
    SELECT 
        CASE 
            WHEN iw."Customer" IN ('KROGER', 'FRED MEYER') THEN 'KROGER'
            ELSE iw."Customer"
        END AS customer,
        i.PRODUCTLINE,
        i.PRODUCTTYPE,
        i.PRODUCTVARIANT,
        i.PRODUCTFORM,
        iw."Updated Week Ending Date",
        SUM(iw."POS Units") AS units,
        SUM(iw."POS Dollars") AS sales
    FROM POS_TEST_DB.SCH_SUMMARY.POS_ITEMWEEKLY_XCHANGE_VIEW iw
    INNER JOIN items i
        ON i.ITEM_NUMBER = iw."Part Number"
    JOIN date_bounds db
        ON iw."Updated Week Ending Date" BETWEEN db.start_date AND db.end_date
    GROUP BY 
        CASE 
            WHEN iw."Customer" IN ('KROGER', 'FRED MEYER') THEN 'KROGER'
            ELSE iw."Customer"
        END,
        i.PRODUCTLINE,
        i.PRODUCTTYPE,
        i.PRODUCTVARIANT,
        i.PRODUCTFORM,
        iw."Updated Week Ending Date"
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
