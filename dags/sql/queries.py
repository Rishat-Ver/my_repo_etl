d_customer_query = """
BEGIN TRANSACTION;

UPDATE public.d_customer
set end_date = '{{ds}}'::date - interval '1 day'
WHERE id IN (SELECT id
             FROM public.d_customer_stg
             WHERE id IS NOT NULL); --обновляем end_date

INSERT INTO public.d_customer(customer_id, first_name, last_name, start_date, end_date)
SELECT stg.customer_id,
       stg.first_name,
       stg.last_name,
       '{{ds}}',
       '9999-12-31'
FROM public.d_customer_stg stg; -- вставляем актуальное инфо

COMMIT TRANSACTION;
"""

d_customer_query_stg = """
WITH
     api_data AS ( --актуальные на дату расчёта данные в источнике
         SELECT DISTINCT
         ON (uol.customer_id)
             uol.customer_id,
             uol.first_name,
             uol.last_name
         FROM public.user_order_log uol
         WHERE
             uol.date_time::date = '{{ds}}'
         ORDER BY uol.customer_id, date_time DESC
)
INSERT INTO public.d_customer_stg (id, customer_id, first_name, last_name)
SELECT --получаем дельту и вставляем данные
       dim.id,
       uol.customer_id,
       uol.first_name,
       uol.last_name
FROM api_data uol
         LEFT JOIN public.d_customer dim ON dim.customer_id = uol.customer_id
    AND '{{ds}}' BETWEEN start_date AND end_date
WHERE (uol.first_name != dim.first_name
    OR uol.last_name != dim.last_name)
   OR dim.customer_id IS NULL;
"""

d_item_stg_query = """
WITH
     api_data AS ( --актуальные на дату расчёта данные в источнике
         SELECT DISTINCT
         ON (uol.item_id)
             uol.item_id,
             uol.item_name
         FROM public.user_order_log uol
         WHERE
             uol.date_time::date = '{{ds}}'
         ORDER BY uol.item_id, date_time DESC
)
INSERT INTO public.d_item_stg (id, item_id, item_name)
SELECT --получаем дельту и вставляем данные
       dim.id,
       uol.item_id,
       uol.item_name
FROM api_data uol
         LEFT join public.d_item dim on dim.item_id = uol.item_id
         and '{{ds}}' BETWEEN start_date AND end_date
WHERE uol.item_name != dim.item_name
   OR dim.item_id IS NULL;
"""

d_item_query = """
BEGIN TRANSACTION;

UPDATE public.d_item
set end_date = '{{ds}}'::date - interval '1 day'
WHERE id IN (SELECT id
             FROM public.d_item_stg
             WHERE id IS NOT NULL); --обновляем end_date

INSERT INTO public.d_item(item_id, item_name, start_date, end_date)
SELECT stg.item_id,
       stg.item_name,
       '{{ds}}',
       '9999-12-31'
FROM public.d_item_stg stg; -- вставляем актуальное инфо

COMMIT TRANSACTION;
"""

d_city_stg_query = """
WITH
     api_data AS ( --актуальные на дату расчёта данные в источнике
         SELECT DISTINCT
         ON (uol.city_id)
             uol.city_id,
             uol.city_name
         FROM public.user_order_log uol
         WHERE
             uol.date_time::date = '{{ds}}'
         ORDER BY uol.city_id, date_time DESC
)
INSERT INTO public.d_city_stg (id, city_id, city_name)
SELECT --получаем дельту и вставляем данные
       dim.id,
       uol.city_id,
       uol.city_name
FROM api_data uol
         LEFT JOIN  public.d_city dim on dim.city_id = uol.city_id
         and '{{ds}}' BETWEEN start_date AND end_date
WHERE uol.city_name != dim.city_name
  OR dim.city_id IS NULL;
"""

d_city_query = """
BEGIN TRANSACTION;

UPDATE public.d_customer
set end_date = '{{ds}}'::date - interval '1 day'
WHERE id IN (SELECT id
             FROM public.d_customer_stg
             WHERE id IS NOT NULL); --обновляем end_date

INSERT INTO public.d_customer(customer_id, first_name, last_name, start_date, end_date)
SELECT stg.customer_id,
       stg.first_name,
       stg.last_name,
       '{{ds}}',
       '9999-12-31'
FROM public.d_customer_stg stg; -- вставляем актуальное инфо

INSERT INTO public.d_customer(customer_id, first_name, last_name,start_date, end_date)
    SELECT DISTINCT
        customer_id,
        'неизвестно',
        'неизвестно' ,
       '{{ds}}',
       '9999-12-31'
    FROM public.user_activity_log uol
    LEFT JOIN public.d_customer dim USING (customer_id)
    WHERE dim.id IS NULL; -- добавляем клиентов, которые пока не совершали заказов

COMMIT TRANSACTION;
"""

f_order_query = """
INSERT INTO public.f_order (order_id, create_date, customer_id, city_id, item_id, quantity, payment_amount)
SELECT uniq_id         AS order_id,
       date_time::date AS create_date,
       cus.id      AS customer_id,
       c.id         AS city_id,
       it.id        AS item_id,
       quantity,
       payment_amount
FROM public.user_order_log uol
         JOIN public.d_city c on c.city_id = uol.city_id  and '{{ds}}' BETWEEN c.start_date AND c.end_date
         JOIN public.d_customer cus ON cus.customer_id = uol.customer_id and '{{ds}}' BETWEEN cus.start_date AND cus.end_date
         JOIN public.d_item it on it.item_id = uol.item_id  and '{{ds}}' BETWEEN it.start_date AND it.end_date
WHERE uol.date_time::date = '{{ds}}'
"""

f_activity_query = """

INSERT INTO public.f_activity (activity_id, create_date, customer_id, action_id, quantity)
SELECT uniq_id         AS activity_id,
       date_time::date AS create_date,
       cus.id      AS customer_id,
       ual.action_id      ,
       quantity
FROM public.user_activity_log ual
         JOIN public.d_customer cus ON cus.customer_id = ual.customer_id and '{{ds}}' BETWEEN cus.start_date AND cus.end_date
         WHERE ual.date_time::date = '{{ds}}'
"""

customer_report_query = """
INSERT INTO customer_report(customer_id ,first_name, last_name, uniq_actions, different_actions,
                            days_with_actions,
                            total_actions, uniq_orders, days_with_orders,
                            cities_visited, different_items_bought, money_spent)
    WITH
         agg_actions AS (SELECT customer_id,
                       COUNT(DISTINCT ual.activity_id)      AS uniq_actions,
                       COUNT(DISTINCT action_id)        AS different_actions,
                       COUNT(DISTINCT ual.create_date)    AS days_with_actions,
                       SUM(ual.quantity)                AS total_actions
                       FROM f_activity ual
                       GROUP BY customer_id ),
         agg_orders AS (SELECT customer_id,
                   COUNT(DISTINCT uol.order_id)      AS uniq_orders,
                   COUNT(DISTINCT uol.create_date)    AS days_with_orders,
                   COUNT(DISTINCT city_id)          AS cities_visited,
                   COUNT(DISTINCT item_id)          AS different_items_bought,
                   SUM(payment_amount)              AS money_spent
                   FROM f_order uol
                   GROUP BY customer_id )
    SELECT dc.id ,
           dc.first_name ,
           dc.last_name ,
           uniq_actions,
           different_actions,
           days_with_actions,
           total_actions,
           uniq_orders,
           days_with_orders,
           cities_visited,
           different_items_bought,
           money_spent
    FROM agg_actions ual
             FULL JOIN
         agg_orders uol USING (customer_id)
    JOIN d_customer dc on dc.id = coalesce(uol.customer_id, ual.customer_id);
     ON CONFLICT (customer_id) DO UPDATE
     SET
     uniq_actions = EXCLUDED.uniq_actions,
     different_actions = EXCLUDED.different_actions,
     total_actions = EXCLUDED.total_actions,
     uniq_orders = EXCLUDED.uniq_orders,
     days_with_orders = EXCLUDED.days_with_orders,
     cities_visited = EXCLUDED.cities_visited,
     different_items_bought = EXCLUDED.different_items_bought,
     money_spent = EXCLUDED.money_spent;"""
