/* ============================================================================
   GRIDPULSE AI - PHASE 5
   Investigation serving contracts

   Purpose
   -------
   Source-control the deterministic SQL objects used by the Phase 5
   investigation layer. These objects read trusted Gold/Ops serving data only;
   they do not recreate Bronze/Silver/Gold transformation logic.

   Prerequisites
   -------------
   Existing schemas: gold, ops
   Existing objects:
     ops.dq_result
     gold.fact_market_demand_hourly
     gold.fact_zonal_demand_hourly
     gold.fact_generation_hourly
     gold.fact_day_ahead_price_hourly
     gold.fact_realtime_price_5min
     gold.vw_daily_peak_demand
   ============================================================================ */


/* --------------------------------------------------------------------------
   1. Latest persisted Batch DQ result per logical rule
   -------------------------------------------------------------------------- */

CREATE OR ALTER VIEW ops.vw_latest_dq_result
AS
WITH ranked_dq AS (
    SELECT
        run_id,
        source_name,
        dataset_name,
        rule_id,
        rule_category,
        severity,
        status,
        records_checked,
        records_failed,
        observed_value,
        expected_value,
        execution_timestamp,
        details,
        ROW_NUMBER() OVER (
            PARTITION BY
                source_name,
                dataset_name,
                rule_id
            ORDER BY
                execution_timestamp DESC,
                run_id DESC
        ) AS rn
    FROM ops.dq_result
)
SELECT
    run_id,
    source_name,
    dataset_name,
    rule_id,
    rule_category,
    severity,
    status,
    records_checked,
    records_failed,
    observed_value,
    expected_value,
    execution_timestamp,
    details
FROM ranked_dq
WHERE rn = 1;
GO


/* --------------------------------------------------------------------------
   2. Demand investigation contract
   - market_date is required
   - hour_ending is optional; NULL returns the available date scope
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION gold.fn_get_demand
(
    @market_date DATE,
    @hour_ending INT
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        market_date,
        hour_ending,
        ontario_demand_mw,
        market_demand_mw
    FROM gold.fact_market_demand_hourly
    WHERE
        @market_date IS NOT NULL
        AND market_date = @market_date
        AND (
            @hour_ending IS NULL
            OR hour_ending = @hour_ending
        )
);
GO


/* --------------------------------------------------------------------------
   3. Daily peak-demand investigation contract
   - returns every tied peak hour for the requested date
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION gold.fn_get_peak_demand
(
    @market_date DATE
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        market_date,
        hour_ending,
        ontario_demand_mw,
        market_demand_mw
    FROM gold.vw_daily_peak_demand
    WHERE
        @market_date IS NOT NULL
        AND market_date = @market_date
);
GO


/* --------------------------------------------------------------------------
   4. Zonal-demand investigation contract
   - market_date is required
   - hour_ending and zone are optional filters
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION gold.fn_get_zonal_demand
(
    @market_date DATE,
    @hour_ending INT,
    @zone VARCHAR(8000)
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        market_date,
        hour_ending,
        zone,
        zonal_demand_mw
    FROM gold.fact_zonal_demand_hourly
    WHERE
        @market_date IS NOT NULL
        AND market_date = @market_date
        AND (
            @hour_ending IS NULL
            OR hour_ending = @hour_ending
        )
        AND (
            @zone IS NULL
            OR zone = @zone
        )
);
GO


/* --------------------------------------------------------------------------
   5. Generation-mix investigation contract
   - market_date and hour_ending are required
   - fuel_type is an optional source-controlled category filter
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION gold.fn_get_generation_mix
(
    @market_date DATE,
    @hour_ending INT,
    @fuel_type VARCHAR(8000)
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        market_date,
        hour_ending,
        fuel_type,
        output_mwh,
        output_quality_code
    FROM gold.fact_generation_hourly
    WHERE
        @market_date IS NOT NULL
        AND @hour_ending IS NOT NULL
        AND market_date = @market_date
        AND hour_ending = @hour_ending
        AND (
            @fuel_type IS NULL
            OR UPPER(LTRIM(RTRIM(fuel_type))) =
               UPPER(LTRIM(RTRIM(@fuel_type)))
        )
);
GO


/* --------------------------------------------------------------------------
   6. Day-Ahead price investigation contract
   - market_date is required
   - hour_ending is optional
   - negative prices are preserved
   - NULL price components are preserved
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION gold.fn_get_day_ahead_price
(
    @market_date DATE,
    @hour_ending INT
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        market_date,
        hour_ending,
        zonal_price_capped_cad_per_mwh,
        loss_price_capped_cad_per_mwh,
        congestion_price_capped_cad_per_mwh,
        _source_flag AS source_flag
    FROM gold.fact_day_ahead_price_hourly
    WHERE
        @market_date IS NOT NULL
        AND market_date = @market_date
        AND (
            @hour_ending IS NULL
            OR hour_ending = @hour_ending
        )
);
GO


/* --------------------------------------------------------------------------
   7. Day-Ahead vs historical Real-Time comparison contract
   - compares one explicit market key
   - preserves missing and incomplete evidence instead of coercing NULL to zero
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION gold.fn_compare_da_vs_rt_price
(
    @market_date DATE,
    @hour_ending INT,
    @interval INT
)
RETURNS TABLE
AS
RETURN
(
    WITH requested_key AS (
        SELECT
            @market_date AS market_date,
            @hour_ending AS hour_ending,
            @interval AS interval
        WHERE
            @market_date IS NOT NULL
            AND @hour_ending IS NOT NULL
            AND @interval IS NOT NULL
    )
    SELECT
        k.market_date,
        k.hour_ending,
        k.interval,

        da.zonal_price_capped_cad_per_mwh
            AS da_zonal_price_capped_cad_per_mwh,
        da.loss_price_capped_cad_per_mwh
            AS da_loss_price_capped_cad_per_mwh,
        da.congestion_price_capped_cad_per_mwh
            AS da_congestion_price_capped_cad_per_mwh,
        da._source_flag AS da_source_flag,

        rt.zonal_price_capped_cad_per_mwh
            AS rt_zonal_price_capped_cad_per_mwh,
        rt.loss_price_capped_cad_per_mwh
            AS rt_loss_price_capped_cad_per_mwh,
        rt.congestion_price_capped_cad_per_mwh
            AS rt_congestion_price_capped_cad_per_mwh,
        rt._source_flag AS rt_source_flag,

        CASE
            WHEN da.market_date IS NULL
             AND rt.delivery_date IS NULL
                THEN 'MISSING_BOTH'
            WHEN da.market_date IS NULL
                THEN 'MISSING_DAY_AHEAD'
            WHEN rt.delivery_date IS NULL
                THEN 'MISSING_REALTIME'
            WHEN da.zonal_price_capped_cad_per_mwh IS NULL
              OR da.loss_price_capped_cad_per_mwh IS NULL
              OR da.congestion_price_capped_cad_per_mwh IS NULL
              OR rt.zonal_price_capped_cad_per_mwh IS NULL
              OR rt.loss_price_capped_cad_per_mwh IS NULL
              OR rt.congestion_price_capped_cad_per_mwh IS NULL
                THEN 'INCOMPLETE_PRICE_COMPONENTS'
            ELSE 'COMPARABLE'
        END AS comparison_state,

        CASE
            WHEN da.zonal_price_capped_cad_per_mwh IS NOT NULL
             AND rt.zonal_price_capped_cad_per_mwh IS NOT NULL
                THEN rt.zonal_price_capped_cad_per_mwh
                   - da.zonal_price_capped_cad_per_mwh
            ELSE NULL
        END AS zonal_rt_minus_da_cad_per_mwh,

        CASE
            WHEN da.loss_price_capped_cad_per_mwh IS NOT NULL
             AND rt.loss_price_capped_cad_per_mwh IS NOT NULL
                THEN rt.loss_price_capped_cad_per_mwh
                   - da.loss_price_capped_cad_per_mwh
            ELSE NULL
        END AS loss_rt_minus_da_cad_per_mwh,

        CASE
            WHEN da.congestion_price_capped_cad_per_mwh IS NOT NULL
             AND rt.congestion_price_capped_cad_per_mwh IS NOT NULL
                THEN rt.congestion_price_capped_cad_per_mwh
                   - da.congestion_price_capped_cad_per_mwh
            ELSE NULL
        END AS congestion_rt_minus_da_cad_per_mwh

    FROM requested_key AS k
    LEFT JOIN gold.fact_day_ahead_price_hourly AS da
        ON da.market_date = k.market_date
       AND da.hour_ending = k.hour_ending
    LEFT JOIN gold.fact_realtime_price_5min AS rt
        ON rt.delivery_date = k.market_date
       AND rt.delivery_hour = k.hour_ending
       AND rt.interval = k.interval
);
GO


/* --------------------------------------------------------------------------
   8. Batch DQ evidence contract

   Semantic normalization:
   - Batch severity -> rule_outcome
   - Batch status   -> execution_status
   -------------------------------------------------------------------------- */

CREATE OR ALTER FUNCTION ops.fn_get_batch_dq_status
(
    @dataset_name VARCHAR(8000),
    @rule_id VARCHAR(8000)
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        d.source_name,
        d.dataset_name,
        d.rule_id,
        d.rule_category,
        d.severity AS rule_outcome,
        d.status AS execution_status,
        d.records_checked,
        d.records_failed,
        d.observed_value,
        d.expected_value,
        d.execution_timestamp AS evaluated_at,
        d.details,
        d.run_id
    FROM ops.vw_latest_dq_result AS d
    WHERE
        @dataset_name IS NOT NULL
        AND d.dataset_name = @dataset_name
        AND (
            @rule_id IS NULL
            OR d.rule_id = @rule_id
        )
);
GO
