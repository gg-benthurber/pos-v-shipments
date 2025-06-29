SELECT
    ORG.Entity,
    ITEM.Brand,
    CHANNEL.FORECAST_CHANNEL,
    ITEM.Productline,
    ITEM.Producttype,
    ITEM.Productvariant,
    ITEM.Productform,
    CALENDAR.WEEK_END_DATE,
    SUM(CASE 
            WHEN SALE.DATA_SOURCE = 'GP' 
                THEN SALE.TOTAL_PRICE - SALE.EXTENDED_MARKDOWN_AMOUNT
            WHEN SALE.DATA_SOURCE <> 'GP'
                THEN SALE.EXTENED_PRICE
            ELSE 0 
        END) AS SALES,
    SUM(shipped_qty_ea) as QTY
    
FROM 
    BI_ST_LI_METRIC SALE
    JOIN BI_ITEM ITEM ON SALE.ITEM_ID = ITEM.ITEM_ID AND UPPER(ITEM.BRAND) = 'OKEEFFES'
    JOIN BI_ST_LI_DETAIL SALE_DETAILS ON SALE.LINE_ITEM_DETAIL_ID = SALE_DETAILS.LINE_ITEM_DETAIL_ID and SALE_DETAILS.FULFILL_LINE_STATUS_CODE = 'CLOSED'
    JOIN BI_CUSTOMER CUSTOMER ON SALE.SHIP_TO_CUSTOMER_ACCOUNT_NUMBER = CUSTOMER.ACCOUNT_NUMBER 
        AND CUSTOMER.EXLCUDE_FROM_SALES = 0 
    JOIN BI_CALENDAR CALENDAR ON SALE.POST_DATE_KEY = CALENDAR.DATE_KEY 
        AND CALENDAR.fiscal_date between trunc(current_date, 'iw') - 364  and trunc(current_date, 'iw') - 1
    JOIN BI_PL_CHANNEL CHANNEL ON SALE.PL_CHANNEL_ID = CHANNEL.PL_CHANNEL_ID
    JOIN BI_INV_ORG_ENTITY ORG ON SALE.FULFILL_ORG_ID = ORG.INV_ORGANIZATION_ID
        AND ORG.ENTITY = 'The Gorilla Glue Company LLC'
        AND ORG.ORGANIZATION_NAME = 'Gorilla Glue Tri-County'
GROUP BY 
    ORG.Entity, ITEM.Brand, 
    CHANNEL.FORECAST_CHANNEL, 
    ITEM.Productline,
    ITEM.Producttype,
    ITEM.Productvariant,
    ITEM.Productform, 
    CALENDAR.WEEK_END_DATE