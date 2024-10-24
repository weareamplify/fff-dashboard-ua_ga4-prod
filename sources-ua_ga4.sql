CREATE OR REPLACE TABLE `federation-francaise-football.site_billetterie_dataset.ecommerceproduct_agregee_UA_GA4` AS
WITH ga4_events AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS date,
        event_name,
        event_bundle_sequence_id AS session_id,
        ecom_items.item_name AS product,  -- Utilisation de "product" au lieu de "produit"
        ecom_items.quantity AS product_revenue,  -- Utilisation de "product_revenue"
        CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END AS is_purchase
    FROM
        `federation-francaise-football.analytics_429428621.events_*`,
        UNNEST(items) AS ecom_items  -- Décomposer les produits vendus dans l'événement e-commerce
    WHERE
        event_name IN ('purchase')
),

-- Comptage des achats uniques et des achats par produit pour GA4
ga4_purchase_data AS (
    SELECT
        date,
        product,
        COUNT(DISTINCT session_id) AS unique_purchases,  -- Achats uniques par produit
        SUM(product_revenue) AS product_revenue  -- Quantité achetée par produit
    FROM
        ga4_events
    GROUP BY
        date, product
),

-- Sélection des données GA4
ga4_final AS (
  SELECT
      pd.date AS date,
      pd.product AS product,
      pd.unique_purchases AS unique_purchases,
      pd.product_revenue AS product_revenue,
      'GA4' AS source_type
  FROM
      ga4_purchase_data pd
),

-- Sélection des données historiques Universal Analytics (UA)
ua_final AS (
  SELECT
      -- Conversion de la date en type DATE
      CAST(date AS DATE) AS date,
      product,
      CAST(REPLACE(CAST(unique_purchases AS STRING), '\u202f', '') AS FLOAT64) AS unique_purchases,  -- Conversion en FLOAT64
      CAST(REPLACE(CAST(product_revenue AS STRING), '\u202f', '') AS FLOAT64) AS product_revenue,  -- Conversion en FLOAT64
      'UA' AS source_type
  FROM
      `federation-francaise-football.site_billetterie_dataset.ecommerce_historique_ua`
)

-- Union des deux sources (GA4 et UA)
SELECT * FROM ga4_final
UNION ALL
SELECT * FROM ua_final
ORDER BY date, product;
17 h 17
Requête sources agregee ua ga4
CREATE OR REPLACE TABLE `federation-francaise-football.site_billetterie_dataset.sources_agregee_UA_GA4` AS
WITH ga4_events AS (
    SELECT
        PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS date,
        event_name,
        event_bundle_sequence_id AS session_id,
        traffic_source.source AS source,  -- Ajout de la dimension source
        CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END AS is_page_view,
        CASE WHEN event_name = 'session_start' THEN 1 ELSE 0 END AS is_session_start
    FROM
        `federation-francaise-football.analytics_429428621.events_*`
    WHERE
        event_name IN ('session_start', 'page_view', 'user_engagement')
),

-- Comptage des événements session_start par date et source pour GA4
ga4_session_counts AS (
    SELECT
        date,
        source,
        COUNT(*) AS total_sessions
    FROM
        ga4_events
    WHERE
        is_session_start = 1
    GROUP BY
        date, source
),

-- Comptage des pages vues par date et source pour GA4
ga4_pageviews AS (
    SELECT
        date,
        source,
        COUNT(*) AS total_pageviews
    FROM
        ga4_events
    WHERE
        is_page_view = 1
    GROUP BY
        date, source
),

-- Calcul des données des sessions pour le taux de rebond pour GA4
ga4_sessions_data AS (
  SELECT
    PARSE_DATE('%Y%m%d', CAST(event_date AS STRING)) AS date,
    event_bundle_sequence_id AS session_id,
    traffic_source.source AS source,  -- Ajout de la dimension source
    COUNTIF(event_name = 'page_view') AS page_views_per_session,
    COUNTIF(event_name = 'user_engagement') AS engagement_events
  FROM
    `federation-francaise-football.analytics_429428621.events_*`
  WHERE
    event_name IN ('page_view', 'user_engagement')
  GROUP BY
    date, session_id, source
),

-- Calcul du taux de rebond par date et source pour GA4
ga4_bounce_rate_data AS (
  SELECT
    date,
    source,
    COUNTIF(page_views_per_session = 1 AND engagement_events = 0) / COUNT(*) AS bounce_rate
  FROM
    ga4_sessions_data
  GROUP BY
    date, source
),

-- Sélection des données GA4
ga4_final AS (
  SELECT
      sc.date AS date,
      sc.source AS source,
      COALESCE(sc.total_sessions, 0) AS sessions,
      COALESCE(pv.total_pageviews, 0) AS pageviews,
      COALESCE(brd.bounce_rate, 0) * 100 AS bounce_rate,
      'GA4' AS source_type
  FROM
      ga4_session_counts sc
  LEFT JOIN
      ga4_pageviews pv
  ON
      sc.date = pv.date AND sc.source = pv.source
  LEFT JOIN
      ga4_bounce_rate_data brd
  ON
      sc.date = brd.date AND sc.source = brd.source
),

-- Sélection des données historiques Universal Analytics (UA)
ua_final AS (
  SELECT
      -- Conversion de la date en type DATE
      CAST(date AS DATE) AS date,
      source,
      CAST(REPLACE(CAST(sessions AS STRING), '\u202f', '') AS INT64) AS sessions,
      CAST(REPLACE(CAST(pageviews AS STRING), '\u202f', '') AS INT64) AS pageviews,
      -- Gestion des valeurs du taux de rebond
      CAST(
        CASE 
            -- Convertir bounce_rate en STRING avant d'appliquer REGEXP_CONTAINS
            WHEN REGEXP_CONTAINS(CAST(bounce_rate AS STRING), r'^[0-9.,<%]+$') THEN 
                REPLACE(REPLACE(CAST(bounce_rate AS STRING), '<0.01%', '0.0001'), ',', '.')
            ELSE '0'
        END 
        AS FLOAT64
      ) * 100 AS bounce_rate,
      'UA' AS source_type
  FROM
      `federation-francaise-football.site_billetterie_dataset.sources_historique_ua_billetterie`
)

-- Union des deux sources (GA4 et UA)
SELECT * FROM ga4_final
UNION ALL
SELECT * FROM ua_final
ORDER BY date, source;