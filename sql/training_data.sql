WITH 
USER_BASIC_INFO AS (
    SELECT 
        * except(rn)
    FROM (
        SELECT 
            user_pseudo_id,
            -- device
            device.category AS category,
            device.mobile_brand_name AS mobile_brand_name,
            device.operating_system AS operating_system,
            -- geo
            geo.country AS country,
            -- traffic_source
            traffic_source.medium AS medium,
            traffic_source.name AS name,
            traffic_source.source AS source,
            ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY TIMESTAMP_MICROS(event_timestamp) DESC) AS rn
        FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
        WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
    )
    WHERE rn = 1
),
EVENT_CNT AS (
    SELECT 
        user_pseudo_id,
        COUNT(DISTINCT (SELECT value.int_value FROM UNNEST(event_params) WHERE key='ga_session_id')) AS session_cnt,
        COUNT(IF(event_name = 'page_view', 1, NULL)) AS page_view_cnt,
        COUNT(IF(event_name = 'view_item', 1, NULL)) AS view_item_cnt,
        COUNT(IF(event_name = 'view_promotion', 1, NULL)) AS view_promotion_cnt,
        COUNT(IF(event_name = 'add_to_cart', 1, NULL)) AS add_to_cart_cnt,
        COUNT(IF(event_name = 'begin_checkout', 1, NULL)) AS begin_checkout_cnt,
        -- 
        count(if(event_name = 'add_payment_info', 1, null)) as add_payment_info_cnt,
        count(if(event_name = 'add_shipping_info', 1, null)) as add_shipping_info_cnt,
        count(if(event_name = 'click', 1, null)) as click_cnt,
        count(if(event_name = 'first_visit', 1, null)) as first_visit_cnt,
        count(if(event_name = 'purchase', 1, null)) as purchase_cnt,
        count(if(event_name = 'scroll', 1, null)) as scroll_cnt,
        count(if(event_name = 'select_item', 1, null)) as select_item_cnt,
        count(if(event_name = 'select_promotion', 1, null)) as select_promotion_cnt,
        count(if(event_name = 'session_start', 1, null)) as session_start_cnt,
        count(if(event_name = 'user_engagement', 1, null)) as user_engagement_cnt,
        count(if(event_name = 'view_item', 1, null)) as view_item_cnt,
        count(if(event_name = 'view_search_results', 1, null)) as view_search_results_cnt
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
    WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
    GROUP BY user_pseudo_id
),
CHECKOUT AS (
    SELECT
        user_pseudo_id,
        -- items
        COUNT(item.item_name) AS purchase_item_cnt,
        SUM(item.price) AS total_spending,
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, UNNEST(items) item
    WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
    AND event_name = 'begin_checkout'
    GROUP BY user_pseudo_id
),
USER_PURCHASE AS (
    SELECT
        user_pseudo_id,
        COUNTIF(item_category = 'Apparel') AS Apparel,
        COUNTIF(item_category = 'Drinkware') AS Drinkware,
        COUNTIF(item_category = 'Office') AS Office,
        COUNTIF(item_category = 'Electronics Accessories') AS Electronics_Accessories,
        COUNTIF(item_category = 'Gift Cards') AS Gift_Cards,
        COUNTIF(item_category = 'Notebooks & Journals') AS Notebooks_Journals,
        COUNTIF(item_category = 'New') AS News,
        COUNTIF(item_category = 'Bags') AS Bags,
        COUNTIF(item_category = 'Campus Collection') AS Campus_Collection,
        COUNTIF(item_category = 'Stationery') AS Stationery,
        COUNTIF(item_category = 'Clearance') AS Clearance,
        COUNTIF(item_category = 'Lifestyle') AS Lifestyle,
        COUNTIF(item_category = 'Google') AS Google,
        COUNTIF(item_category = 'Shop by Brand') AS Shop_by_Brand,
        COUNTIF(item_category = 'Accessories') AS Accessories,
        COUNTIF(item_category = 'Small Goods') AS Small_Goods,
        COUNTIF(item_category = 'Writing Instruments') AS Writing_Instruments,
        COUNTIF(item_category = 'Uncategorized Items') AS Uncategorized_Items,
        COUNTIF(item_category = 'Fun') AS Fun
    FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, UNNEST(items) item
    WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
    AND event_name = 'begin_checkout'
    GROUP BY user_pseudo_id
)
SELECT 
    * except(purchase_item_cnt, total_spending),
    ifnull(purchase_item_cnt, 0) as purchase_item_cnt,
    ifnull(total_spending, 0) as total_spending,
    SAFE_DIVIDE(total_spending, purchase_cnt) as spending_per_purchase
FROM USER_BASIC_INFO
LEFT JOIN EVENT_CNT
USING(user_pseudo_id)
LEFT JOIN CHECKOUT
USING(user_pseudo_id)
LEFT JOIN USER_PURCHASE
USING(user_pseudo_id)
WHERE total_spending is not null