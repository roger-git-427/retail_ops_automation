BEGIN;

-- Makes the synthetic dataset reproducible.
SELECT setseed(0.42);

WITH
dates AS (
    SELECT gs::date AS order_date
    FROM generate_series(
        DATE '2025-10-01',
        DATE '2026-09-26',
        INTERVAL '1 day'
    ) AS gs
),

skus (
    sku,
    base_quantity,
    unit_price,
    annual_trend
) AS (
    VALUES
        ('MANGO-12PK',   18.0, 25.50::numeric, 0.18),
        ('ACAI-24PK',    12.0, 39.50::numeric, 0.12),
        ('COCONUT-6PK',  15.0, 18.75::numeric, 0.08)
),

retailers (
    retailer_code,
    retailer_name,
    demand_factor
) AS (
    VALUES
        ('WMT', 'Walmart Mexico', 1.35),
        ('SOR', 'Soriana',        1.05),
        ('CHE', 'Chedraui',       0.95),
        ('COS', 'Costco Mexico',  1.20),
        ('OXX', 'Oxxo',           0.75)
),

generated_orders AS (
    SELECT
        d.order_date,
        s.sku,
        r.retailer_code,
        r.retailer_name,
        s.unit_price,

        GREATEST(
            1,
            ROUND(
                s.base_quantity
                * r.demand_factor

                -- Weekly seasonality
                * CASE EXTRACT(ISODOW FROM d.order_date)::integer
                    WHEN 1 THEN 0.90
                    WHEN 5 THEN 1.12
                    WHEN 6 THEN 1.22
                    WHEN 7 THEN 1.10
                    ELSE 1.00
                  END

                -- Seasonal variation during the year
                * (
                    1
                    + 0.15 * SIN(
                        2 * PI()
                        * EXTRACT(DOY FROM d.order_date)
                        / 365.0
                    )
                  )

                -- Gradual growth trend
                * (
                    1
                    + s.annual_trend
                    * (
                        (d.order_date - DATE '2025-10-01')
                        / 365.0
                      )
                  )

                -- Occasional promotion spike
                * CASE
                    WHEN random() < 0.05 THEN 1.65
                    ELSE 1.00
                  END

                -- Random demand noise
                * (0.85 + random() * 0.30)
            )
        )::integer AS quantity,

        (
            d.order_date::timestamp
            + make_interval(
                hours => 8 + FLOOR(random() * 10)::integer,
                mins  => FLOOR(random() * 60)::integer
            )
        ) AT TIME ZONE 'UTC' AS created_at

    FROM dates d
    CROSS JOIN skus s
    CROSS JOIN retailers r
)

INSERT INTO orders (
    po_number,
    retailer_name,
    sku,
    quantity,
    unit_price,
    status,
    created_at
)
SELECT
    'HIST-'
        || TO_CHAR(order_date, 'YYYYMMDD')
        || '-'
        || REPLACE(sku, '-', '')
        || '-'
        || retailer_code,

    retailer_name,
    sku,
    quantity,
    unit_price,
    'received',
    created_at

FROM generated_orders

ON CONFLICT (po_number) DO NOTHING;

COMMIT;