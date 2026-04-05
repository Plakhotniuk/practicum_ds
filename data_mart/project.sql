/* Проект «Разработка витрины и решение ad-hoc задач»
 * Цель проекта: подготовка витрины данных маркетплейса «ВсёТут»
 * и решение четырех ad hoc задач на её основе
 * 
 * Автор: Плахотнюк Арсений Вячеславович
 * Дата: 04.11.2025
*/



/* Часть 1. Разработка витрины данных */
WITH top_regions AS ( -- ТОП-3 региона по количеству заказов
    SELECT 
        u.region
    FROM ds_ecom.orders o
    JOIN ds_ecom.users u ON o.buyer_id = u.buyer_id
    GROUP BY u.region
    ORDER BY COUNT(o.order_id) DESC
    LIMIT 3
),
orders_with_valid_status AS ( -- заказы только с нужными статусами
    SELECT
        DISTINCT order_id
    FROM ds_ecom.orders
    WHERE order_status IN ('Доставлено', 'Отменено')
),
items_agg AS (
    SELECT
        oi.order_id,
        SUM(COALESCE(oi.price,0) + COALESCE(oi.delivery_cost,0)) AS order_price
    FROM ds_ecom.order_items oi
    JOIN ds_ecom.orders o ON oi.order_id = o.order_id
    WHERE o.order_status = 'Доставлено'
    GROUP BY oi.order_id
),
reviews_agg AS ( -- агрегируем отзывы
    SELECT
        DISTINCT order_id,
        AVG(
            CASE 
                WHEN review_score BETWEEN 0 AND 5 THEN review_score::NUMERIC
                WHEN review_score BETWEEN 10 AND 50 THEN review_score::NUMERIC / 10
                ELSE NULL
            END
        ) AS review_score
    FROM ds_ecom.order_reviews
    GROUP BY order_id
),
payments_agg AS (
    SELECT 
        DISTINCT o.order_id,
        FIRST_VALUE(op.payment_type) OVER(
            PARTITION BY o.order_id
            ORDER BY op.payment_sequential ASC
        ) AS order_first_payment_type,
        MAX(CASE WHEN op.payment_type = 'промокод' THEN 1 ELSE 0 END) OVER (
            PARTITION BY o.order_id
        ) AS used_promo,
        MAX(CASE WHEN op.payment_installments > 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY o.order_id
        ) AS used_installment
    FROM ds_ecom.orders o
    LEFT JOIN ds_ecom.order_payments op ON o.order_id = op.order_id
),
orders_aggregate_info AS ( -- сбор данных на уровне одного заказа
    SELECT
        o.order_id,
        o.buyer_id,
        o.order_status,
        o.order_purchase_ts,
        r.review_score,
        p.order_first_payment_type,
        p.used_promo,
        p.used_installment,
        COALESCE(i.order_price, 0) AS order_price
    FROM orders_with_valid_status ows
    LEFT JOIN ds_ecom.orders o ON ows.order_id = o.order_id 
    LEFT JOIN items_agg i ON o.order_id = i.order_id
    LEFT JOIN payments_agg p ON o.order_id = p.order_id
    LEFT JOIN reviews_agg r ON o.order_id = r.order_id
),
user_first_payment AS (
    SELECT DISTINCT
        o.buyer_id,
        FIRST_VALUE(o.order_first_payment_type) OVER(
            PARTITION BY o.buyer_id 
            ORDER BY o.order_purchase_ts ASC
        ) AS first_payment_type
    FROM orders_aggregate_info o
),
user_activity AS ( -- 1. Базовая информация о клиенте и времени его активности
    SELECT
        u.user_id,
        u.region,
        MIN(oai.order_purchase_ts) AS first_order_ts,
        MAX(oai.order_purchase_ts) AS last_order_ts,
        MAX(oai.order_purchase_ts) - MIN(oai.order_purchase_ts) AS lifetime
    FROM ds_ecom.users u
    JOIN top_regions tr ON u.region = tr.region
    JOIN orders_aggregate_info oai ON u.buyer_id = oai.buyer_id
    LEFT JOIN user_first_payment fp ON u.buyer_id = fp.buyer_id
    GROUP BY u.user_id, u.region
),
orders_info AS ( -- 2. Информация о заказах клиента
    SELECT
        u.user_id,
        u.region,
        COUNT(DISTINCT oai.order_id) AS total_orders,
        AVG(oai.review_score::NUMERIC) AS avg_order_rating,
        COUNT(oai.review_score) AS num_orders_with_rating,
        SUM(CASE WHEN oai.order_status = 'Отменено' THEN 1 ELSE 0 END) AS num_canceled_orders,
        SUM(CASE WHEN oai.order_status = 'Отменено' THEN 1 ELSE 0 END)::NUMERIC
            / NULLIF(COUNT(DISTINCT oai.order_id),0)::NUMERIC AS canceled_orders_ratio
    FROM orders_aggregate_info oai
    JOIN ds_ecom.users u ON oai.buyer_id = u.buyer_id
    JOIN top_regions tr ON u.region = tr.region
    GROUP BY u.user_id, u.region
),
payments_info AS (
    SELECT
        u.user_id,
        u.region,
        SUM(CASE WHEN oai.order_status = 'Доставлено' THEN oai.order_price ELSE 0 END) AS total_order_costs,
        AVG(CASE WHEN oai.order_status = 'Доставлено' THEN oai.order_price END) AS avg_order_cost,
        COUNT(DISTINCT CASE WHEN oai.used_installment = 1 THEN oai.order_id END) AS num_installment_orders,
        SUM(oai.used_promo) AS num_orders_with_promo,
        MAX(CASE WHEN fp.first_payment_type = 'денежный перевод' THEN 1 ELSE 0 END) AS used_money_transfer,
        MAX(CASE WHEN oai.used_installment = 1 THEN 1 ELSE 0 END) AS used_installments,
        MAX(CASE WHEN oai.order_status = 'Отменено' THEN 1 ELSE 0 END) AS used_cancel
    FROM orders_aggregate_info oai
    JOIN ds_ecom.users u ON oai.buyer_id = u.buyer_id
    JOIN top_regions tr ON u.region = tr.region
    LEFT JOIN user_first_payment fp ON u.buyer_id = fp.buyer_id
    GROUP BY u.user_id, u.region
)
SELECT
    ua.user_id,
    ua.region,
    ua.first_order_ts,
    ua.last_order_ts,
    ua.lifetime,
    oi.total_orders,
    oi.avg_order_rating,
    oi.num_orders_with_rating,
    oi.num_canceled_orders,
    oi.canceled_orders_ratio,
    pi.total_order_costs,
    pi.avg_order_cost,
    pi.num_installment_orders,
    pi.num_orders_with_promo,
    pi.used_money_transfer,
    pi.used_installments,
    pi.used_cancel
FROM user_activity ua
LEFT JOIN orders_info oi 
    ON ua.user_id = oi.user_id AND ua.region = oi.region
LEFT JOIN payments_info pi 
    ON ua.user_id = pi.user_id AND ua.region = pi.region;
/* Часть 2. Решение ad hoc задач
 * Для каждой задачи напишите отдельный запрос.
 * После каждой задачи оставьте краткий комментарий с выводами по полученным результатам.
*/

/* Задача 1. Сегментация пользователей 
 * Разделите пользователей на группы по количеству совершённых ими заказов.
 * Подсчитайте для каждой группы общее количество пользователей,
 * среднее количество заказов, среднюю стоимость заказа.
 * 
 * Выделите такие сегменты:
 * - 1 заказ — сегмент 1 заказ
 * - от 2 до 5 заказов — сегмент 2-5 заказов
 * - от 6 до 10 заказов — сегмент 6-10 заказов
 * - 11 и более заказов — сегмент 11 и более заказов
*/
WITH segmented_users AS 
(
SELECT
	user_id,
    total_orders,
    total_order_costs,
    CASE
        WHEN total_orders = 1 THEN '1 заказ'
        WHEN total_orders BETWEEN 2 AND 5 THEN '2–5 заказов'
        WHEN total_orders BETWEEN 6 AND 10 THEN '6–10 заказов'
        WHEN total_orders >= 11 THEN '11 и более заказов'
        ELSE 'без заказов'
    END AS segment
FROM ds_ecom.product_user_features
)
SELECT
    segment,
    COUNT(DISTINCT user_id) AS users_in_segment, -- уникальные пользователи 
    ROUND(AVG(total_orders)::NUMERIC, 2) AS avg_orders_per_user,
    ROUND(CASE WHEN SUM(COALESCE(total_orders,0)) = 0 THEN NULL
         ELSE SUM(COALESCE(total_order_costs,0))::numeric / SUM(COALESCE(total_orders,0))
    END, 2) AS avg_cost_per_order
FROM segmented_users
GROUP BY segment
ORDER BY
    CASE segment
        WHEN '1 заказ' THEN 1
        WHEN '2–5 заказов' THEN 2
        WHEN '6–10 заказов' THEN 3
        WHEN '11 и более заказов' THEN 4
        ELSE 5
    END;

/* Напишите краткий комментарий с выводами по результатам задачи 1.
 *  Около 96% пользователей сделали 1 заказ и около 3% пользователей сделали 2-5 заказов
 * Самый большой средний чек у группы сделавших 1 заказ
 * Средняя сумма заказа убывает с ростом числа заказов
*/



/* Задача 2. Ранжирование пользователей 
 * Отсортируйте пользователей, сделавших 3 заказа и более, по убыванию среднего чека покупки.  
 * Выведите 15 пользователей с самым большим средним чеком среди указанной группы.
*/
SELECT
    user_id,
    region,
    total_orders,
    avg_order_cost,
    total_order_costs,
    avg_order_rating,
    num_orders_with_rating
FROM ds_ecom.product_user_features
WHERE total_orders >= 3
ORDER BY avg_order_cost DESC NULLS LAST
LIMIT 15;
/* Напишите краткий комментарий с выводами по результатам задачи 2.
 * Самый высокий средний чек и суммарная стоиость заказов у пользователя из Санкт-Петербурга,
 * Можно заметить, что в топ 15 Москвичей больше всего: 66% от общего числа
 * Количество заказов у всех из топа: от 3 до 5.
 * Клиенты с большим средним чеков в основном довольны заказами: 60% средних оценок больше 4.5
*/



/* Задача 3. Статистика по регионам. 
 * Для каждого региона подсчитайте:
 * - общее число клиентов и заказов;
 * - среднюю стоимость одного заказа;
 * - долю заказов, которые были куплены в рассрочку;
 * - долю заказов, которые были куплены с использованием промокодов;
 * - долю пользователей, совершивших отмену заказа хотя бы один раз.
*/
SELECT
    region,
    COUNT(user_id) AS clients_count, -- число клиентов
    SUM(COALESCE(total_orders,0)) AS total_orders, -- общее число заказов
    ROUND(
        CASE WHEN SUM(COALESCE(total_orders,0)) = 0 THEN NULL
             ELSE SUM(COALESCE(total_order_costs,0))::numeric / SUM(COALESCE(total_orders,0))
        END
    , 4) AS avg_cost_per_order, -- средняя стоимость одного заказа в регионе:
    ROUND(
        CASE WHEN SUM(COALESCE(total_orders,0)) = 0 THEN NULL
             ELSE SUM(COALESCE(num_installment_orders,0))::numeric / SUM(COALESCE(total_orders,0))
        END
    , 4) * 100 || '%' AS share_installment_orders, -- доля заказов в рассрочку
    ROUND(
        CASE WHEN SUM(COALESCE(total_orders,0)) = 0 THEN NULL
             ELSE SUM(COALESCE(num_orders_with_promo,0))::numeric / SUM(COALESCE(total_orders,0))
        END
    , 4) * 100 || '%' AS share_promo_orders, -- доля заказов с промокодом
    ROUND(AVG(CASE WHEN used_cancel = 1 THEN 1.0 ELSE 0.0 END), 4) * 100 || '%' AS share_users_with_cancel -- доля пользователей, которые отменяли хотя бы один заказ
FROM ds_ecom.product_user_features
GROUP BY region
ORDER BY total_orders DESC;

/* Напишите краткий комментарий с выводами по результатам задачи 3.
 * Больше всего клиентов и заказов в Москве: почти в 4 раза по этим показателям превосходство над остальными городами из топ 3
 * Средняя стоимость заказов имеет небольшой разброс по городам.
 * Больше половины заказов в рассрочку в Санкт-Петербурге и Новосибирской области, в Москве этот показатель ниже половины.
 * Для заказов по промокодам примерно одинаковая: от 3.7% до 4.1%.
 * Доля отказов по заказам крайне низкая, менее 1% для всех регионов из топа
*/



/* Задача 4. Активность пользователей по первому месяцу заказа в 2023 году
 * Разбейте пользователей на группы в зависимости от того, в какой месяц 2023 года они совершили первый заказ.
 * Для каждой группы посчитайте:
 * - общее количество клиентов, число заказов и среднюю стоимость одного заказа;
 * - средний рейтинг заказа;
 * - долю пользователей, использующих денежные переводы при оплате;
 * - среднюю продолжительность активности пользователя.
*/
SELECT
    EXTRACT(MONTH FROM first_order_ts)::int AS month,
    COUNT(DISTINCT user_id) AS clients_count,
    SUM(COALESCE(total_orders,0)) AS total_orders,
    ROUND(
        CASE WHEN SUM(COALESCE(total_orders,0)) = 0 THEN NULL
             ELSE SUM(COALESCE(total_order_costs,0))::numeric / SUM(COALESCE(total_orders,0))
        END
    , 2) AS avg_cost_per_order,
    ROUND(AVG(avg_order_rating)::numeric, 2) AS avg_order_rating_per_user, -- средний рейтинг заказов по пользователю (не взвешенный)
    ROUND(AVG(CASE WHEN used_money_transfer = 1 THEN 1.0 ELSE 0.0 END), 4) * 100 || '%' AS share_users_using_money_transfer, -- доля пользователей, использующих денежный перевод
    ROUND(AVG(COALESCE(EXTRACT('day' FROM lifetime),0))::numeric, 2) AS avg_lifetime_days
FROM ds_ecom.product_user_features
WHERE EXTRACT(YEAR FROM first_order_ts) = 2023
GROUP BY month
ORDER BY month;
/* Напишите краткий комментарий с выводами по результатам задачи 4.
 * Максимальная активность клиентов происходит в ноябре и декабре: существенный отрыв по заказам и пользователям
 * По показателю среднего чека и среднего рейтинга заказов сильных колебаний не наблюдается от месяца к месяцу
 * Процент пользователей, которые используют денеждыми переводами для оплаты заказов весь год держится на уровне 20% (min = 19%, max = 22.1%)
 * Дольше всего пользователи активничают в январе.
*/