DROP TABLE IF EXISTS metrics;

CREATE TABLE
    metrics AS
WITH
    manufactures_snapshot AS (
        SELECT
            t.CasinoManufacturerId,
            t.CasinoManufacturerName
        FROM
            (
                SELECT
                    *,
                    MAX(FromDate) OVER (
                        PARTITION BY
                            CasinoManufacturerId
                    ) AS LastCreationDate
                FROM
                    casino_manufacturers
            ) t
        WHERE
            t.LastCreationDate <= t.FromDate
    ),
    filtered_casino_daily AS (
        SELECT
            *
        FROM
            casino_daily
        WHERE date BETWEEN $date_from AND $date_to
    ),
    all_currency_rates AS (
        SELECT
            date,
            FromCurrencyId AS FromCurrencyId,
            ToCurrencyId AS ToCurrencyId,
            EuroRate AS ConversionRate
        FROM
            currency_rates
        UNION ALL
        SELECT
            date,
            ToCurrencyId AS FromCurrencyId,
            FromCurrencyId AS ToCurrencyId,
            1 / EuroRate AS ConversionRate
        FROM
            currency_rates
    ),
    granular_metrics AS (
        SELECT
            d.Date,
            d.UserID,
            u.Country,
            u.Sex,
            DATEDIFF ('YEAR', u.BirthDate, d.Date) AS Age,
            CASE
                WHEN UPPER(u.VIPStatus) = 'NOT VIP' THEN UPPER(u.VIPStatus)
                ELSE UPPER(
                    REGEXP_REPLACE (u.VIPStatus, '(\b)*\s(\b)*', '', 'g')
                )
            END AS VIPStatus,
            m.CasinoManufacturerName,
            p.CasinoProviderName,
            d.GGR * r.ConversionRate AS GGR,
            d.Returns * r.ConversionRate AS Returns
        FROM
            filtered_casino_daily d
            LEFT JOIN manufactures_snapshot m on d.CasinomanufacturerId = m.CasinomanufacturerId
            INNER JOIN users u on u.user_id = d.UserID
            LEFT JOIN casino_providers p on d.CasinoProviderId = p.CasinoProviderId
            INNER JOIN all_currency_rates r on d.CurrencyId = r.FromCurrencyId
            AND d.Date = r.Date
    ),
    granular_metrics_age_group as (
        SELECT
            *,
            CASE
                WHEN Age IS NULL THEN 'Unknown'
                WHEN Age < 21 THEN 'Under 18'
                WHEN Age <= 26 THEN '21-26'
                WHEN Age <= 32 THEN '27-32'
                WHEN Age <= 40 THEN '33-40'
                WHEN Age <= 50 THEN '41-50'
                ELSE '50+'
            END AS AgeGroup
        FROM
            granular_metrics
    )
SELECT
    Date,
    Country,
    Sex,
    AgeGroup,
    VIPStatus,
    CasinoManufacturerName,
    CasinoProviderName,
    SUM(GGR) AS GGR,
    SUM(Returns) AS Returns
FROM
    granular_metrics_age_group
GROUP BY
    Date,
    Country,
    Sex,
    AgeGroup,
    VIPStatus,
    CasinoManufacturerName,
    CasinoProviderName
ORDER BY
    Date,
    AGeGroup,
    VIPStatus