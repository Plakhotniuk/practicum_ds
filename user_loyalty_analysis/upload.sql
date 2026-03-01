-- Настройка параметра synchronize_seqscans важна для проверки
WITH set_config_precode AS (
  SELECT set_config('synchronize_seqscans', 'off', true)
),
purchases_enriched AS (
  SELECT
    p.user_id,
    p.device_type_canonical,
    p.order_id,
    p.created_dt_msk as order_dt,
    p.created_ts_msk as order_ts,
    p.currency_code,
    p.revenue,
    p.tickets_count,
    (
      created_dt_msk::date
      - LAG(created_dt_msk::date) OVER (
          PARTITION BY user_id
          ORDER BY created_dt_msk
        )
    )::int AS days_since_prev,
    p.event_id,
    e.event_name_code as event_name,
    e.event_type_main,
    p.service_name,
    r.region_name,
    c.city_name
  FROM afisha.purchases p
  INNER JOIN afisha.events e ON e.event_id = p.event_id
  LEFT JOIN afisha.city c ON c.city_id = e.city_id
  LEFT JOIN afisha.regions r ON r.region_id = c.region_id
  WHERE device_type_canonical IN ('mobile', 'desktop') AND e.event_type_main != 'фильм'
)
SELECT *
FROM purchases_enriched
ORDER BY user_id ASC;
