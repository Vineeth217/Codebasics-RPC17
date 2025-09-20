                                                                           #Business Request 1

WITH city_month_circulation AS (
    SELECT 
        c.city AS city_name,
        DATE_FORMAT(STR_TO_DATE(fps.Month, '%d-%m-%Y'), '%Y-%m') AS month_yyyy_mm,
        SUM(fps.Net_Circulation) AS total_net_circulation
    FROM rpc17.fact_print_sales fps
    JOIN rpc17.dim_city c 
        ON fps.City_ID = c.city_id
    WHERE fps.Year BETWEEN 2019 AND 2024
    GROUP BY c.city, DATE_FORMAT(STR_TO_DATE(fps.Month, '%d-%m-%Y'), '%Y-%m')
),
circulation_changes AS (
    SELECT 
        city_name,
        month_yyyy_mm,
        total_net_circulation,
        LAG(total_net_circulation) OVER (
            PARTITION BY city_name ORDER BY month_yyyy_mm
        ) AS prev_net_circ
    FROM city_month_circulation
)
SELECT 
    city_name,
    month_yyyy_mm AS month,
    total_net_circulation AS net_circulation,
    (prev_net_circ - total_net_circulation) AS drop_in_circulation
FROM circulation_changes
WHERE prev_net_circ IS NOT NULL
ORDER BY drop_in_circulation DESC
LIMIT 3;

                                                                            #Business Request 2

SELECT
  far.year,
  dac.ad_category_id    AS category_name,
  SUM(far.ad_revenue_inr)    AS category_revenue,
  yr_tot.total_revenue_year,
  ROUND( SUM(far.ad_revenue_inr) / yr_tot.total_revenue_year * 100, 2 ) AS pct_of_year_total
FROM rpc17.fact_ad_revenue AS far
JOIN rpc17.dim_ad_category AS dac
  ON far.ad_category = dac.ad_category_id
JOIN (
  SELECT year, SUM(ad_revenue_inr) AS total_revenue_year
  FROM rpc17.fact_ad_revenue
  WHERE year BETWEEN 2019 AND 2024
  GROUP BY year
) AS yr_tot
  ON far.year = yr_tot.year
WHERE far.year BETWEEN 2019 AND 2024
GROUP BY far.year, dac.standard_ad_category
HAVING SUM(far.ad_revenue_inr) > 0.5 * yr_tot.total_revenue_year
ORDER BY far.year, pct_of_year_total DESC;

                                                                             #Business Request 3

WITH city_efficiency AS (
  SELECT
    c.city AS city_name,
    SUM(fps.`Copies Printed`) AS copies_printed_2024,
    SUM(fps.`Net_Circulation`) AS net_circulation_2024,
    ROUND(
      SUM(fps.`Net_Circulation`) / NULLIF(SUM(fps.`Copies Printed`), 0),
      4
    )                                    AS efficiency_ratio
  FROM rpc17.fact_print_sales fps
  JOIN rpc17.dim_city c ON fps.city_id = c.city_id
  WHERE fps.year = 2024
  GROUP BY c.city
),
ranked AS (
  SELECT
    city_name,
    copies_printed_2024,
    net_circulation_2024,
    efficiency_ratio,
    RANK() OVER (ORDER BY efficiency_ratio DESC) AS efficiency_rank_2024
  FROM city_efficiency
)
SELECT *
FROM ranked
WHERE efficiency_rank_2024 <= 5
ORDER BY efficiency_rank_2024;

                                                                              #Business Request 4

SELECT 
    dc.city AS city_name,
    q1.internet_penetration AS internet_rate_q1_2021,
    q4.internet_penetration AS internet_rate_q4_2021,
    round((q4.internet_penetration - q1.internet_penetration),2)AS delta_internet_rate
FROM dim_city dc
JOIN fact_city_readiness q1
    ON dc.city_id = q1.city_id
   AND q1.Year = 2021
   AND q1.Quarter = 'Q1'
JOIN fact_city_readiness q4
    ON dc.city_id = q4.city_id
   AND q4.Year = 2021
   AND q4.Quarter = 'Q4'
ORDER BY delta_internet_rate DESC;

																			#Business Request 5

WITH yearly AS (
  SELECT
    c.city AS city_name,
    p.Year,
    SUM(p.Net_Circulation) AS yearly_net_circulation,
    SUM(a.ad_revenue_inr)   AS yearly_ad_revenue
  FROM fact_print_sales p
  JOIN dim_edition e ON p.edition_id = e.edition_id
  JOIN fact_ad_revenue a ON e.edition_id = a.edition_id AND a.Year = p.Year
  JOIN dim_city c ON c.city_id = p.City_ID
  WHERE p.Year BETWEEN 2019 AND 2024
  GROUP BY c.city, p.Year
),
with_prev AS (
  SELECT
    city_name,
    Year,
    yearly_net_circulation,
    yearly_ad_revenue,
    LAG(yearly_net_circulation) OVER (PARTITION BY city_name ORDER BY Year) AS prev_net,
    LAG(yearly_ad_revenue)      OVER (PARTITION BY city_name ORDER BY Year) AS prev_ad
  FROM yearly
)
SELECT
  city_name,
  Year,
  yearly_net_circulation,
  yearly_ad_revenue,
  CASE WHEN yearly_net_circulation < prev_net THEN 'Yes' ELSE 'No' END AS is_declining_print,
  CASE WHEN yearly_ad_revenue < prev_ad THEN 'Yes' ELSE 'No' END AS is_declining_ad_revenue,
  CASE WHEN yearly_net_circulation < prev_net 
            AND yearly_ad_revenue < prev_ad
       THEN 'Yes' ELSE 'No' END AS is_declining_both
FROM with_prev
WHERE Year > 2019
ORDER BY city_name, Year;

