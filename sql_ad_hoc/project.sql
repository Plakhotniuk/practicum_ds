/* Проект «Секреты Тёмнолесья»
 * Цель проекта: изучить влияние характеристик игроков и их игровых персонажей 
 * на покупку внутриигровой валюты «райские лепестки», а также оценить 
 * активность игроков при совершении внутриигровых покупок
 * 
 * Автор: Плахотнюк Арсений Вячеславович
 * Дата: 03.11.2025
*/

-- Часть 1. Исследовательский анализ данных
-- Задача 1. Исследование доли платящих игроков

-- 1.1. Доля платящих пользователей по всем данным:
WITH payer_count AS 
(
SELECT
	COUNT(id) AS p_count
FROM fantasy.users
WHERE payer = 1
),
total_count AS 
(
SELECT
	COUNT(id) AS tot_count
FROM fantasy.users
)
SELECT 
	tot_count,
	p_count,
	p_count::NUMERIC / tot_count AS payers_part
FROM total_count, payer_count;
-- 1.2. Доля платящих пользователей в разрезе расы персонажа:
WITH payer_count_r AS 
(
SELECT
	r.race,
	COUNT(id) AS p_count
FROM fantasy.users AS u 
LEFT JOIN fantasy.race AS r ON u.race_id = r.race_id 
WHERE payer = 1
GROUP BY r.race
),
total_count_r AS 
(
SELECT
	r.race,
	COUNT(id) AS tot_count
FROM fantasy.users AS u 
LEFT JOIN fantasy.race AS r ON u.race_id = r.race_id 
GROUP BY r.race
)
SELECT 
	tcr.race,
	tot_count,
	p_count,
	p_count::NUMERIC / tot_count AS payers_part
FROM total_count_r AS tcr
LEFT JOIN payer_count_r AS pcr ON tcr.race = pcr.race;
-- Задача 2. Исследование внутриигровых покупок
-- 2.1. Статистические показатели по полю amount:
SELECT 
	COUNT(amount) AS buy_count,
	SUM(amount) AS tot_amount,
	MIN(amount) AS min_amount,
	MAX(amount) AS max_amount,
	AVG(amount) AS avg_amount,
	PERCENTILE_CONT(0.5) WITHIN GROUP ( ORDER BY amount ) AS median_amount,
	STDDEV(amount) AS stddev_amount
FROM fantasy.events;
-- 2.2: Аномальные нулевые покупки:
WITH total_count AS
(
SELECT
	COUNT(amount) AS total_count
FROM fantasy.events
),
zero_count AS 
(
SELECT 
	COUNT(amount) AS zero_count
FROM fantasy.events
WHERE amount = 0
)
SELECT 
	zero_count,
	zero_count::NUMERIC / total_count AS zero_part
FROM total_count, zero_count;
-- 2.3: Популярные эпические предметы:
WITH total_sells AS (
SELECT
	COUNT(transaction_id) AS sells_count
FROM fantasy.events
WHERE amount > 0
),
items_sells AS (
SELECT
	i.game_items AS game_items,
	COUNT(e.transaction_id) AS item_sells_count
FROM total_sells AS t, fantasy.items AS i
LEFT JOIN fantasy.events AS e ON i.item_code = e.item_code 
WHERE e.amount > 0
GROUP BY i.game_items
),
total_buyers AS (
SELECT
	COUNT(DISTINCT id) AS total_buyers
FROM fantasy.events
WHERE amount > 0
),
users_buy_item AS (
SELECT
	i.game_items AS game_items,
	COUNT(DISTINCT e.id) AS item_buyers
FROM total_buyers AS tb, fantasy.items AS i
LEFT JOIN fantasy.events AS e ON i.item_code = e.item_code 
WHERE amount > 0
GROUP BY i.game_items
)
SELECT 
	s.game_items,
	s.item_sells_count, -- общее число внутриигровых продаж
	s.item_sells_count::NUMERIC / t.sells_count AS item_buy_part, -- доля внутриигровых продаж
	ubi.item_buyers::NUMERIC / tb.total_buyers AS item_buyers_part  -- доля игроков которое хоть раз покупали предмет среди всех покупателей
FROM total_sells AS t, total_buyers AS tb, items_sells AS s 
INNER JOIN users_buy_item AS ubi ON ubi.game_items = s.game_items
ORDER BY item_buyers_part DESC;
-- Часть 2. Решение ad hoc-задачbи
-- Задача: Зависимость активности игроков от расы персонажа:
-- Часть 2. Решение ad hoc-задачbи
-- Задача: Зависимость активности игроков от расы персонажа:
WITH user_register AS (
    SELECT
        r.race AS race,
        COUNT(DISTINCT u.id) AS users_count -- количество зарегистированных пользователей в разрезе рас
    FROM fantasy.race AS r
    INNER JOIN fantasy.users AS u ON r.race_id = u.race_id
    GROUP BY r.race
),
user_purchases_stats AS (
    SELECT
        u.id AS user_id,
        r.race AS race,
        COUNT(e.transaction_id) AS purchases_count, -- количество покупок каждого пользователя для каждой расы
        SUM(e.amount) AS sum_amount -- суммарная стоимость покупок для каждого пользователя для каждой расы
    FROM fantasy.race AS r
    INNER JOIN fantasy.users AS u ON r.race_id = u.race_id
    INNER JOIN fantasy.events AS e ON u.id = e.id
    WHERE e.amount > 0  -- фильтрация нулевых покупок
    GROUP BY u.id, r.race
),
race_stats AS (
    SELECT
        race,
        COUNT(DISTINCT user_id) AS buyers_count, -- количество покупателей в разрезе рас
        AVG(purchases_count::NUMERIC) AS avg_purchases_per_buyer,-- среднее число покупок пользователей в разрезе каждой расы
        AVG(sum_amount::NUMERIC) AS avg_sum_amount -- средней суммарной стоимостью всех покупок пользователей в разрезе каждой расы
    FROM user_purchases_stats
    GROUP BY race
),
payers_count_among_buyers AS (
    SELECT 
        r.race AS race,
        COUNT(DISTINCT u.id) AS payers_count  -- количество платящих клиентов среди тех, кто совершает внутриигровые покупки
    FROM fantasy.race AS r
    INNER JOIN fantasy.users AS u ON r.race_id = u.race_id
    INNER JOIN fantasy.events AS e ON u.id = e.id
    WHERE u.payer = 1 AND e.amount > 0
    GROUP BY r.race
)
SELECT
    ur.race,
    ur.users_count,  -- общее количество зарегистрированных игроков
    rs.buyers_count, -- количество игроков, которые совершают внутриигровые покупки
    rs.buyers_count::NUMERIC / ur.users_count AS buyers_share, -- доля покупателей от общего количества;
    pcab.payers_count::NUMERIC / rs.buyers_count AS payers_among_buyers_share, --  доля платящих игроков среди игроков, которые совершили внутриигровые покупки
    (
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY payers_count::NUMERIC / buyers_count)
        FROM payers_count_among_buyers, race_stats
    ) AS median_payers_among_buyers_share,
    rs.avg_purchases_per_buyer AS avg_purchases_per_buyer, -- среднее количество покупок на одного игрока, совершившего внутриигровые покупки;
    (
        SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY avg_purchases_per_buyer)
        	FROM payers_count_among_buyers, race_stats
    ) AS median_avg_purchases_per_buyer,
    rs.avg_sum_amount AS avg_sum_amount, -- средняя стоимость одной покупки на одного игрока, совершившего внутриигровые покупки;
    rs.avg_sum_amount /  rs.avg_purchases_per_buyer AS avg_purchase_amount -- средняя суммарная стоимость всех покупок на одного игрока, совершившего внутриигровые покупки.
FROM user_register AS ur
INNER JOIN race_stats AS rs ON ur.race = rs.race
INNER JOIN payers_count_among_buyers AS pcab ON ur.race = pcab.race
ORDER BY ur.race;