/*Временная таблица для исходных данных*/
CREATE TEMP TABLE all_data AS
    SELECT 
        DATE(o.submited_at_dttm) AS metric_date,
        di.category_name AS category,
        c.city AS city,
        oi.order_id,
        o.customer_id AS buyer,
        /*Смотрим, был ли возврат, если да, то нельзя учитывать кол-во товаров в метрике*/
        CASE
            WHEN oi.return_flg = True THEN 0
            ELSE oi.num 
        END AS item_count,
        oi.return_flg AS is_returned,
        /*Смотрим время доставки в часах, для этого из последней даты доставки вычитаем дату создания заказа*/
        EXTRACT(EPOCH FROM (MAX(dt.delivery_dttm) OVER(PARTITION BY dt.order_id) - o.submited_at_dttm)) / 3600 AS delivery_time_hours,
        /*Считаем выручку с продажи айтемов, если был возврат, то не учитываем*/
        CASE
            WHEN oi.return_flg = True THEN 0
            ELSE oi.num * (ip.retail_price - ip.cost_price)
        END AS item_rev
    FROM public_aaa.aaa_bi_item_price ip 
    JOIN public_aaa.aaa_bi_order_items oi 
        ON oi.item_id = ip.item_id
        /*Сразу фильтруем по актуальным ценам*/
        AND ip.active_to_dttm = '2999-12-31 00:00:00'
    JOIN public_aaa.aaa_bi_orders o
        ON o.order_id = oi.order_id
    JOIN public_aaa.aaa_bi_items i
        ON oi.item_id  = i.item_id
    JOIN public_aaa.aaa_bi_dict_item_categories di 
        ON i.category_id = di.category_id
    JOIN public_aaa.aaa_bi_customers c 
        ON o.customer_id = c.customer_id
    JOIN public_aaa.aaa_bi_order_delivery_time dt
        ON dt.order_id = o.order_id
    /*Проверка*/
    WHERE 
        o.submited_at_dttm IS NOT NULL
        AND di.category_name IS NOT NULL
        AND c.city IS NOT NULL
        AND ip.retail_price IS NOT NULL
        AND ip.cost_price IS NOT NULL 
    ORDER BY 1;

/*Считаем все метрики*/
CREATE TEMP TABLE base_metrics AS 
    /*Считаем выручку*/
    SELECT
        'revenue' AS metric_name,
        metric_date,
        category,
        city,
        SUM(item_rev) AS metric_value
    FROM all_data
    GROUP BY 2, 3, 4

    UNION ALL
    /*Считаем кол-во байеров*/
    SELECT
        'number_of_buyers' AS metric_name,
        metric_date,
        category,
        city,
        COUNT(DISTINCT buyer) AS metric_value
    FROM all_data
    GROUP BY 2, 3, 4

    UNION ALL 
    /*Считаем кол-во проданных айтемов*/
    SELECT
        'number_of_items_sold' AS metric_name,
        metric_date,
        category,
        city,
        SUM(item_count) AS metric_value
    FROM all_data
    GROUP BY 2, 3, 4

    UNION ALL 
    /*Считаем кол-во заказов*/
    SELECT
        'number_of_orders' AS metric_name,
        metric_date,
        category,
        city,
        COUNT(DISTINCT order_id) AS metric_value
    FROM all_data
    GROUP BY 2, 3, 4

    UNION ALL
    /*Считаем кол-во возвратов*/
    SELECT
        'number_of_returns' AS metric_name,
        metric_date,
        category,
        city,
        SUM(is_returned::int) AS metric_value
    FROM all_data
    GROUP BY 2, 3, 4

    UNION ALL
    /*Считаем время доставки*/
    SELECT
        'delivery_time' AS metric_name,
        metric_date,
        category,
        city,
        AVG(delivery_time_hours) AS metric_value
    FROM all_data
    GROUP BY 2, 3, 4;

CREATE TEMP TABLE metrics_with_lags AS
    SELECT 
        *,
        /*Добавляем прошлые значения для WoW/MoM/YoY*/
        LAG(metric_value, 7) OVER (
            PARTITION BY metric_name, category, city
            ORDER BY metric_date
        ) AS prior_week_value,
        
        LAG(metric_value, 30) OVER (
            PARTITION BY metric_name, category, city
            ORDER BY metric_date
        ) AS prior_month_value,
        
        LAG(metric_value, 364) OVER (
            PARTITION BY metric_name, category, city
            ORDER BY metric_date
        ) AS prior_year_value
    FROM base_metrics;

SELECT 
    metric_name,
    metric_date,
    category,
    city,
    metric_value,
    prior_week_value,
    prior_month_value,
    prior_year_value
FROM metrics_with_lags
ORDER BY metric_name, metric_date, category, city
