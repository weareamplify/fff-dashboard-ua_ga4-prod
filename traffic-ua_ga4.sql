CREATE OR REPLACE TABLE `federation-francaise-football.site_billetterie_dataset.agrégée_UA_GA4` AS
WITH ga4_events AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS date,
        event_name,
        event_bundle_sequence_id AS session_id,
        CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END AS is_page_view,
        CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END AS is_session_start
    FROM
        `federation-francaise-football.analytics_429428621.events_*`
    WHERE
        event_name IN ('session_start', 'page_view', 'user_engagement')
),

-- Comptage des sessions par date pour GA4
ga4_session_counts AS (
    SELECT
        date,
        COUNT(*) AS total_sessions
    FROM
        ga4_events
    WHERE
        is_session_start = 1
    GROUP BY
        date
),

-- Comptage des pages vues par date pour GA4
ga4_pageviews AS (
    SELECT
        date,
        COUNT(*) AS total_pageviews
    FROM
        ga4_events
    WHERE
        is_page_view = 1
    GROUP BY
        date
),

-- Calcul des sessions pour le taux de rebond pour GA4
ga4_sessions_data AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS date,
        event_bundle_sequence_id AS session_id,
        COUNTIF(event_name = 'page_view') AS page_views_per_session,
        COUNTIF(event_name = 'user_engagement') AS engagement_events
    FROM
        `federation-francaise-football.analytics_429428621.events_*`
    WHERE
        event_name IN ('page_view', 'user_engagement')
    GROUP BY
        date, session_id
),

-- Calcul du taux de rebond par date pour GA4
ga4_bounce_rate_data AS (
    SELECT
        date,
        COUNTIF(page_views_per_session = 1 AND engagement_events = 0) / COUNT(*) AS bounce_rate
    FROM
        ga4_sessions_data
    GROUP BY
        date
),

-- Sélection des données GA4
ga4_final AS (
    SELECT
        sc.date AS date,
        COALESCE(sc.total_sessions, 0) AS sessions,
        COALESCE(pv.total_pageviews, 0) AS pageviews,
        COALESCE(brd.bounce_rate, 0) * 100 AS bounce_rate,
        'GA4' AS source
    FROM
        ga4_session_counts sc
    LEFT JOIN
        ga4_pageviews pv
    ON
        sc.date = pv.date
    LEFT JOIN
        ga4_bounce_rate_data brd
    ON
        sc.date = brd.date
),

-- Sélection des données historiques Universal Analytics (UA)
ua_final AS (
  SELECT
      -- Conversion de la date en type DATE
      CAST(date AS DATE) AS date,
      CAST(REPLACE(CAST(sessions AS STRING), '\u202f', '') AS INT64) AS sessions,
      CAST(REPLACE(CAST(pageviews AS STRING), '\u202f', '') AS INT64) AS pageviews,
      -- Gestion des valeurs du taux de rebond
      CAST(
        CASE 
            WHEN REGEXP_CONTAINS(CAST(bounce_rate AS STRING), r'^[0-9.,<%]+$') THEN 
                REPLACE(REPLACE(CAST(bounce_rate AS STRING), '<0.01%', '0.0001'), ',', '.')
            ELSE '0'
        END AS FLOAT64
      ) * 100 AS bounce_rate,
      'UA' AS source
  FROM
      `federation-francaise-football.site_billetterie_dataset.historique_ua_billetterie`
)

-- Union des deux sources (GA4 et UA)
SELECT * FROM ga4_final
UNION ALL
SELECT * FROM ua_final
ORDER BY date;