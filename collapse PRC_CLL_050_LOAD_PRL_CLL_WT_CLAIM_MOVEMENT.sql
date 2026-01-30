CREATE OR REPLACE PROCEDURE CLAIMS_DEFAULT_000_COD.PRC_CLL_050_LOAD_PRL_CLL_WT_CLAIM_MOVEMENT()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
/***************************************************************************************************
Procedure:          CLAIMS_DEFAULT_000_COD.PRC_CLL_050_LOAD_PRL_CLL_WT_CLAIM_MOVEMENT()
Create Date:        21 May 2025
Author:             Vojco Kraljevski
Description:        This procedure prepares the dataset for Claim Movements by loading it into the
                    table CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT. This dataset is required as input to
                    generate the final dataset for the CLAIMS_DEFAULT.CLL_FACT_CLAIM_MOVEMENT table
                    in the Claim Light data product. This procedure is implemented as a
                    lift-and-shift of the already existing procedure [ODS].[usp_LoadClaimMovement]
                    from the Legacy BI system.
Call by:            A task with requisite role and permissions provided.
Usage:              CALL CLAIMS_DEFAULT_000_COD.PRC_CLL_050_LOAD_PRL_CLL_WT_CLAIM_MOVEMENT();
****************************************************************************************************
SUMMARY OF CHANGES
Date(dd Mmm yyyy)   Author              Comments
------------------- ------------------- ------------------------------------------------------------
13 Oct 2025         Vojco Kraljevski    v1.6 - Unnecessary code removed, as per SF2-23880
                                        and code clean-up
10 Oct 2025         Praveena            v1.5 - Add _SOURCE Columns for Settlement Currency Fields
                                        using Finance_MDM Rate Logic as per SF2- 22339
22 Sep 2025         Reena               v1.4 - To match BI ODS counts, added filters on Claim
                                        as per SF2-22360:
                                        - NVL(c.incident_report,0) = 0
                                        - NVL(c.is_retired,0) = 0
                                        --(c.source_system = 'ClaimCenter')
18 Aug 2025         Vojco Kraljevski    v1.3 - Resolved bug for unexpected settlement currencies
                                        and their conversion rate to USD
11 Aug 2025         Vojco Kraljevski    v1.2 - Added filter IS_ACTIVE = 1 when reading from
                                        the STG views
22 May 2025         Vojco Kraljevski    v1.1 - Resolved the non-deterministic calculation of the
                                        algorithm output by further defining the order of claim
                                        movements and specifying partitions in the final step.
21 May 2025         Vojco Kraljevski    v1.0 - Initial script
***************************************************************************************************/
BEGIN

    -- *********************************************************************************************
    --   Step 1: Prepare the session temporary table for the claim movement data.
    --           This table will get populated with SCM and CC movs
    -- *********************************************************************************************

    CREATE OR REPLACE TEMPORARY TABLE CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE (
        claim_movement_id NUMBER(38,0) AUTOINCREMENT,
        exposure_reference VARCHAR,
        sequence_number NUMBER(38,0),
        movement_reference VARCHAR,
        settlement_currency VARCHAR,
        settlement_source_currency VARCHAR, --added new col as per SF2-22339
        original_currency VARCHAR,
        movement_group_id NUMBER(38,0) NOT NULL,
        movement_group_sequence_id NUMBER(38,0) NOT NULL,
        sequence_number_order_id NUMBER(38,0) NOT NULL,
        outstanding_original_ccy_to_settlement_ccy_rate NUMBER(19,12) NOT NULL,
        paid_original_ccy_to_settlement_ccy_rate NUMBER(19,12) NOT NULL,
        movement_date TIMESTAMP_TZ,
        to_date_outstanding_indemnity_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        to_date_outstanding_fees_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        to_date_outstanding_defence_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        to_date_outstanding_indemnity_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        to_date_outstanding_indemnity_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        to_date_outstanding_fees_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        to_date_outstanding_fees_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        to_date_outstanding_defence_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        to_date_outstanding_defence_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        movement_paid_indemnity_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_paid_fees_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_paid_defence_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_paid_indemnity_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_paid_indemnity_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        movement_paid_fees_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_paid_fees_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        movement_paid_defence_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_paid_defence_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        movement_type VARCHAR,
        movement_outstanding_indemnity_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_outstanding_fees_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_outstanding_defence_in_original_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_outstanding_indemnity_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_outstanding_indemnity_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        movement_outstanding_fees_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,
        movement_outstanding_fees_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        movement_outstanding_defence_in_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  
        movement_outstanding_defence_in_source_settlement_ccy NUMBER(19,4) NOT NULL DEFAULT 0,  --added new col as per SF2-22339
        does_not_erode_reserves BOOLEAN,
        indemnity_reserve_in_settlement_ccy NUMBER(19,4),
        fees_reserve_in_settlement_ccy NUMBER(19,4),
        defence_reserve_in_settlement_ccy NUMBER(19,4),
        payment_id VARCHAR,
        payment_type VARCHAR,
        signed_line NUMBER(20,4),
        transaction_tracking_status VARCHAR,
        transfer_status VARCHAR,
        movement_net_payment_amount_in_original_ccy NUMBER(20,4),
        movement_net_payment_amount_in_settlement_ccy NUMBER(20,4),
        movement_tax_amount_in_original_ccy NUMBER(20,4),
        movement_tax_amount_in_settlement_ccy NUMBER(20,4),
        movement_creation_date TIMESTAMP_TZ
    );

    INSERT INTO CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE (
        exposure_reference,
        sequence_number,
        movement_reference,
        settlement_currency,
        settlement_source_currency, --added new col as per SF2-22339
        original_currency,
        movement_group_id,
        movement_group_sequence_id,
        sequence_number_order_id,
        outstanding_original_ccy_to_settlement_ccy_rate,
        paid_original_ccy_to_settlement_ccy_rate,
        movement_date,
        to_date_outstanding_indemnity_in_original_ccy,
        to_date_outstanding_fees_in_original_ccy,
        to_date_outstanding_defence_in_original_ccy,
        to_date_outstanding_indemnity_in_settlement_ccy,
        to_date_outstanding_indemnity_in_source_settlement_ccy,  --added new col as per SF2-22339
        to_date_outstanding_fees_in_settlement_ccy,
        to_date_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
        to_date_outstanding_defence_in_settlement_ccy,
        to_date_outstanding_defence_in_source_settlement_ccy,  --added new col as per SF2-22339
        movement_paid_indemnity_in_original_ccy,
        movement_paid_fees_in_original_ccy,
        movement_paid_defence_in_original_ccy,
        movement_paid_indemnity_in_settlement_ccy,
        movement_paid_indemnity_in_source_settlement_ccy, --added new col as per SF2-22339
        movement_paid_fees_in_settlement_ccy,
        movement_paid_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
        movement_paid_defence_in_settlement_ccy,
        movement_paid_defence_in_source_settlement_ccy,  --added new col as per SF2-22339
        movement_type,
        movement_outstanding_indemnity_in_original_ccy,
        movement_outstanding_fees_in_original_ccy,
        movement_outstanding_defence_in_original_ccy,
        movement_outstanding_indemnity_in_settlement_ccy,
        movement_outstanding_indemnity_in_source_settlement_ccy,  --added new col as per SF2-22339
        movement_outstanding_fees_in_settlement_ccy,
        movement_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
        movement_outstanding_defence_in_settlement_ccy,
        movement_outstanding_defence_in_source_settlement_ccy,  --added new col as per SF2-22339
        does_not_erode_reserves,
        indemnity_reserve_in_settlement_ccy,
        fees_reserve_in_settlement_ccy,
        defence_reserve_in_settlement_ccy,
        payment_id,
        payment_type,
        signed_line,
        transaction_tracking_status,
        transfer_status,
        movement_net_payment_amount_in_original_ccy,
        movement_net_payment_amount_in_settlement_ccy,
        movement_tax_amount_in_original_ccy,
        movement_tax_amount_in_settlement_ccy,
        movement_creation_date
    )
    SELECT
        t.exposure_reference AS exposure_reference,
        t.sequence_number AS sequence_number,
        t.movement_reference AS movement_reference,
        t.settlement_currency AS settlement_currency,
        t.settlement_source_currency as settlement_source_currency, --added new col as per SF2-22339
        t.original_currency AS original_currency,
        DENSE_RANK() OVER (
            ORDER BY 
                t.exposure_reference,
                t.settlement_currency) AS movement_group_id,
        ROW_NUMBER() OVER (
            PARTITION BY 
                t.exposure_reference,
                t.settlement_currency
            ORDER BY  
                CASE 
                    WHEN (t.movement_type = 'SCM') THEN t.movement_date
                    -- IFNULL(movement_period, movement_date) (commented out as per original)
                END,
                -- Specifically for claims which have YA, YB and YC movements, YC needs to come 
                -- before YB. This only applies to multi-original currency claims, where these movements
                -- indicate the change from the original CCY to EUR
                CASE 
                    WHEN (t.movement_type = 'SCM' AND t.movement_reference LIKE 'YA%') THEN 1
                    WHEN (t.movement_type = 'SCM' AND t.movement_reference LIKE 'YC%') THEN 2
                    WHEN (t.movement_type = 'SCM' AND t.movement_reference LIKE 'YB%') THEN 3
                END,
                t.sequence_number,
                t.movement_reference,
                t.original_currency) AS movement_group_sequence_id,
            0 AS sequence_number_order_id,  -- updated below (dependant on movement_sequence_number_id)
            t.outstanding_original_ccy_to_settlement_ccy_rate AS outstanding_original_ccy_to_settlement_ccy_rate,
            t.paid_original_ccy_to_settlement_ccy_rate AS paid_original_ccy_to_settlement_ccy_rate,
            IFNULL(t.movement_date, '1753-01-01'::DATE) AS movement_date,
            IFNULL(t.to_date_outstanding_indemnity_in_original_ccy, 0) AS to_date_outstanding_indemnity_in_original_ccy,
            IFNULL(t.to_date_outstanding_fees_in_original_ccy, 0) AS to_date_outstanding_fees_in_original_ccy,
            IFNULL(t.to_date_outstanding_defence_in_original_ccy, 0) AS to_date_outstanding_defence_in_original_ccy,
            IFNULL(t.to_date_outstanding_indemnity_in_settlement_ccy, 0) * t.calculated_settlement_to_usd_rate AS  to_date_outstanding_indemnity_in_settlement_ccy,
            IFNULL(t.to_date_outstanding_indemnity_in_settlement_ccy, 0) AS to_date_outstanding_indemnity_in_source_settlement_ccy,  --added new col AS per SF2-22339
            IFNULL(t.to_date_outstanding_fees_in_settlement_ccy, 0) * t.calculated_settlement_to_usd_rate AS to_date_outstanding_fees_in_settlement_ccy,
            IFNULL(t.to_date_outstanding_fees_in_settlement_ccy, 0) AS  to_date_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
            IFNULL(t.to_date_outstanding_defence_in_settlement_ccy, 0) * t.calculated_settlement_to_usd_rate AS to_date_outstanding_defence_in_settlement_ccy,
            IFNULL(t.to_date_outstanding_defence_in_settlement_ccy, 0) AS to_date_outstanding_defence_in_source_settlement_ccy,  --added new col as per SF2-22339
            IFNULL(t.movement_paid_indemnity_in_original_ccy, 0) AS movement_paid_indemnity_in_original_ccy,
            IFNULL(t.movement_paid_fees_in_original_ccy, 0) AS movement_paid_fees_in_original_ccy,
            IFNULL(t.movement_paid_defence_in_original_ccy, 0) AS movement_paid_defence_in_original_ccy,
            IFNULL(t.movement_paid_indemnity_in_settlement_ccy, 0) * t.calculated_settlement_to_usd_rate AS movement_paid_indemnity_in_settlement_ccy,
            IFNULL(t.movement_paid_indemnity_in_settlement_ccy,0) AS movement_paid_indemnity_in_source_settlement_ccy, --added new col as per SF2-22339
            IFNULL(t.movement_paid_fees_in_settlement_ccy, 0) * t.calculated_settlement_to_usd_rate AS movement_paid_fees_in_settlement_ccy,
            IFNULL(t.movement_paid_fees_in_settlement_ccy, 0) AS movement_paid_fees_in_source_settlement_ccy, --added new col as per SF2-22339
            IFNULL(t.movement_paid_defence_in_settlement_ccy, 0) * t.calculated_settlement_to_usd_rate AS movement_paid_defence_in_settlement_ccy,
            IFNULL(t.movement_paid_defence_in_settlement_ccy, 0) AS movement_paid_defence_in_source_settlement_ccy,  --added new col as per SF2-22339
            IFNULL(t.movement_type, '') AS movement_type,
            IFNULL(t.movement_outstanding_indemnity_in_original_ccy, 0) AS movement_outstanding_indemnity_in_original_ccy,
            IFNULL(t.movement_outstanding_fees_in_original_ccy, 0) AS movement_outstanding_fees_in_original_ccy,
            IFNULL(t.movement_outstanding_defence_in_original_ccy, 0) AS movement_outstanding_defence_in_original_ccy,
            IFNULL(t.movement_outstanding_indemnity_in_settlement_ccy, 0) * calculated_settlement_to_usd_rate AS movement_outstanding_indemnity_in_settlement_ccy,
            IFNULL(t.movement_outstanding_indemnity_in_settlement_ccy,0) AS movement_outstanding_indemnity_in_source_settlement_ccy,  --added new col as per SF2-22339
            IFNULL(t.movement_outstanding_fees_in_settlement_ccy, 0) * calculated_settlement_to_usd_rate AS movement_outstanding_fees_in_settlement_ccy,
            IFNULL(t.movement_outstanding_fees_in_settlement_ccy, 0) AS movement_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
            IFNULL(t.movement_outstanding_defence_in_settlement_ccy, 0) * calculated_settlement_to_usd_rate AS movement_outstanding_defence_in_settlement_ccy,
            IFNULL(t.movement_outstanding_defence_in_settlement_ccy, 0) AS movement_outstanding_defence_in_source_settlement_ccy,  --added new col as per SF2-22339
            IFNULL(t.does_not_erode_reserves, 0) as does_not_erode_reserves,
            t.indemnity_reserve_in_settlement_ccy AS indemnity_reserve_in_settlement_ccy,
            t.fees_reserve_in_settlement_ccy AS fees_reserve_in_settlement_ccy,
            t.defence_reserve_in_settlement_ccy AS defence_reserve_in_settlement_ccy,
            t.payment_id AS payment_id,
            t.payment_type AS payment_type,
            t.signed_line AS signed_line,
            t.transaction_tracking_status AS transaction_tracking_status,
            t.transfer_status AS transfer_status,
            IFNULL(t.movement_net_payment_amount_in_original_ccy, 0) AS movement_net_payment_amount_in_original_ccy,
            IFNULL(t.movement_net_payment_amount_in_settlement_ccy, 0) AS movement_net_payment_amount_in_settlement_ccy,
            IFNULL(t.movement_tax_amount_in_original_ccy, 0) AS movement_tax_amount_in_original_ccy,
            IFNULL(t.movement_tax_amount_in_settlement_ccy, 0) AS movement_tax_amount_in_settlement_ccy,
            t.movement_creation_date
    FROM (
        SELECT 
            cm.source_system as source_system,
            ROW_NUMBER() OVER (
                PARTITION BY
                    cm.claim_exposure_source_id
                ORDER BY
                    IFNULL(cm.movement_period, cm.movement_date) ASC,
                    cm.movement_date ASC,
                    cm.sequence_number ASC, -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                    cm.movement_reference_source_id ASC, -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                    cm.settlement_currency ASC, -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                    cm.original_currency ASC -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                ) AS sequence_number,
            CASE
                WHEN (cm.settlement_currency IN ('EUR', 'USD', 'CAD', 'GBP')) THEN 1
                WHEN (IFNULL(cm.original_ccy_to_group_ccy_rate, 0) = 0) THEN 1
                ELSE er_cm_sc.ratio_to_usd
            END AS calculated_settlement_to_usd_rate,
            cm.movement_reference_source_id AS movement_reference_source_id,
            CASE 
                WHEN (IFNULL(cee.scm_reference, '') = '') THEN IFNULL(cm.movement_type, '') 
                ELSE cm.movement_reference_source_id
            END AS movement_reference,  -- according to V5 code - in order to pass regression
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.outstanding_original_ccy_to_settlement_ccy_rate, 1)
                ELSE 1
            END AS outstanding_original_ccy_to_settlement_ccy_rate,  -- MultiCCY change
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.paid_original_ccy_to_settlement_ccy_rate, 1)
                ELSE 1
            END AS paid_original_ccy_to_settlement_ccy_rate,  -- MultiCCY change
            cm.movement_date AS movement_date,
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.movement_reserve_indemnity_amount_in_original_ccy, 0)
                ELSE 0 
            END AS to_date_outstanding_indemnity_in_original_ccy,  -- NULL marked as REMOVED
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.movement_reserve_fees_amount_in_original_ccy, 0)
                ELSE 0 
            END AS to_date_outstanding_fees_in_original_ccy,  -- NULL marked as REMOVED
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.movement_reserve_defence_amount_in_original_ccy, 0)
                ELSE 0 
            END AS to_date_outstanding_defence_in_original_ccy,  -- NULL marked as REMOVED
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.movement_paid_indemnity_in_original_ccy, 0)
                -- WHEN movement_type = 'Recovery' THEN IFNULL(cm.movement_paid_indemnity_in_original_ccy, 0) * -1  -- added for Recovery to be as in V5
                ELSE IFNULL(cm.to_date_paid_indemnity_in_reserving_ccy, 0)
            END AS movement_paid_indemnity_in_original_ccy,
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.movement_paid_fees_in_original_ccy, 0)
                ELSE IFNULL(cm.to_date_paid_fees_in_reserving_ccy, 0)
            END AS movement_paid_fees_in_original_ccy,
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.movement_paid_defence_in_original_ccy, 0)
                ELSE IFNULL(cm.to_date_paid_defence_in_reserving_ccy, 0)
            END AS movement_paid_defence_in_original_ccy,
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.to_date_outstanding_indemnity_in_reserving_ccy, 0)
                ELSE 0 
            END AS to_date_outstanding_indemnity_in_settlement_ccy,  -- 0 updated below
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.to_date_outstanding_fees_in_reserving_ccy, 0)
                ELSE 0 
            END AS to_date_outstanding_fees_in_settlement_ccy,  -- 0 updated below
            CASE  -- MovementType = 'SCM' is from migration, from UI will come only with Reserve and Payment even if is SCM
                WHEN (cm.movement_type = 'SCM') THEN IFNULL(cm.to_date_outstanding_defence_in_reserving_ccy, 0)
                ELSE 0 
            END AS to_date_outstanding_defence_in_settlement_ccy,  -- 0 updated below
            IFNULL(cm.to_date_paid_indemnity_in_reserving_ccy, 0) AS movement_paid_indemnity_in_settlement_ccy,  -- need to be remapped in phase 2 after DataContract is extended
            IFNULL(cm.to_date_paid_fees_in_reserving_ccy, 0) AS movement_paid_fees_in_settlement_ccy,  -- need to be remapped in phase 2 after DataContract is extended
            IFNULL(cm.to_date_paid_defence_in_reserving_ccy, 0) AS movement_paid_defence_in_settlement_ccy,  -- need to be remapped in phase 2 after DataContract is extended
            IFNULL(cm.movement_type, '') AS movement_type,
            IFNULL(cm.to_date_outstanding_indemnity_in_reserving_ccy, 0) AS movement_outstanding_indemnity_in_original_ccy,
            IFNULL(cm.to_date_outstanding_fees_in_reserving_ccy, 0) AS movement_outstanding_fees_in_original_ccy,
            IFNULL(cm.to_date_outstanding_defence_in_reserving_ccy, 0) AS movement_outstanding_defence_in_original_ccy,
            IFNULL(cm.to_date_outstanding_indemnity_in_reserving_ccy, 0) AS movement_outstanding_indemnity_in_settlement_ccy,  -- need to be remapped in phase 2 after DataContract is extended
            IFNULL(cm.to_date_outstanding_fees_in_reserving_ccy, 0) AS movement_outstanding_fees_in_settlement_ccy,  -- need to be remapped in phase 2 after DataContract is extended
            IFNULL(cm.to_date_outstanding_defence_in_reserving_ccy, 0) AS movement_outstanding_defence_in_settlement_ccy,  -- need to be remapped in phase 2 after DataContract is extended
            cm.claim_exposure_source_id AS exposure_reference,
            IFNULL(cme.transaction_created_on, cm.movement_date) AS movement_creation_date,  -- cm.movement_date
            CASE 
                WHEN (cm.movement_type = 'SCM') THEN 1 
                ELSE cm.does_not_erode_reserves
            END AS does_not_erode_reserves,
            IFNULL(cm.to_date_outstanding_indemnity_in_reserving_ccy, 0) AS indemnity_reserve_in_settlement_ccy,
            IFNULL(cm.to_date_outstanding_fees_in_reserving_ccy, 0) AS fees_reserve_in_settlement_ccy,
            IFNULL(cm.to_date_outstanding_defence_in_reserving_ccy, 0) AS defence_reserve_in_settlement_ccy,
            cme.payment_id AS payment_id,
            cme.payment_type AS payment_type,
            cme.signed_line AS signed_line,
            cme.transaction_tracking_status AS transaction_tracking_status,
            cme.transfer_status AS transfer_status,
            cm.net_payment_amount_in_original_ccy AS movement_net_payment_amount_in_original_ccy,
            cm.net_payment_amount_in_settlement_ccy AS movement_net_payment_amount_in_settlement_ccy,
            cm.tax_amount_in_original_ccy AS movement_tax_amount_in_original_ccy,
            cm.tax_amount_in_settlement_ccy AS movement_tax_amount_in_settlement_ccy,
            CASE
                WHEN (cm.settlement_currency IN ('EUR', 'USD', 'CAD', 'GBP')) THEN cm.settlement_currency
                ELSE 'USD'
            END AS settlement_currency,
            cm.settlement_currency as settlement_source_currency, --added new col as per SF2-22339
            IFNULL(CASE
                       WHEN (cm.movement_type = 'SCM') THEN cm.original_currency
                       WHEN (cm.movement_type <> 'SCM') THEN cm.settlement_currency
                   END, 'USD') AS original_currency         --MultiCCY change --
        FROM UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSUREMOVEMENT cm
        LEFT JOIN (SELECT
                       er.currency_code_name,
                       1 / er.rate_to_usd as ratio_to_usd,
                   FROM FINANCE_MDM.EXCHANGE_RATES er
                   WHERE (er.rate_type_id = 3) -- Lloyd's PIM
                   QUALIFY ROW_NUMBER() OVER (PARTITION BY er.currency_code_name ORDER BY er.effective_from_date DESC, er.id DESC) = 1) er_cm_sc
            ON (er_cm_sc.currency_code_name = cm.settlement_currency)
        INNER JOIN UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSUREMOVEMENTEXTENSION cme
            ON (cme.claim_exposure_source_id = cm.claim_exposure_source_id)
           AND (cme.source_system = cm.source_system)
           AND (cme.movement_reference_source_id = cm.movement_reference_source_id)
           AND (cme.sequence_number = cm.sequence_number)
           AND (cme.original_currency = cm.original_currency)
           AND (cme.settlement_currency = cm.settlement_currency)
           AND (cme.is_active = 1) -- Added on 2025-08-11
        INNER JOIN UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSURE ce
            ON (ce.claim_exposure_source_id = cm.claim_exposure_source_id)
           AND (ce.source_system = cm.source_system)
           AND (ce.is_active = 1) -- Added on 2025-08-11
        LEFT JOIN UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSUREEXTENSION cee
            ON (cee.claim_exposure_source_id = ce.claim_exposure_source_id)
           AND (cee.source_system = ce.source_system)
           AND (cee.is_active = 1) -- Added on 2025-08-11
        INNER JOIN UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIM c
            ON (c.claim_source_id = ce.claim_source_id)
           AND (c.source_system = ce.source_system)
           AND (c.is_active = 1) -- Added on 2025-08-11
        WHERE (cm.source_system = 'ClaimCenter')
          AND (cm.is_active = 1) -- Added on 2025-08-11
          AND (
               -- All SCM transactions go to ODS
               (cm.movement_type = 'SCM')
               OR
               -- All Non-SCM transactions from Migration (transfer_status = 'Migration') 
               -- and only completed ones from online (have transfer_status = 'Transaction Complete')
               ((cm.movement_type <> 'SCM') AND (IFNULL(transfer_status, '') IN ('Migration', 'Transaction Complete')))
              )
          AND (IFNULL(c.incident_report, 0) = 0)---Added filters as per SF2-22360 on 22/09/2025
          AND (IFNULL(c.is_retired, 0) = 0)-----Added filters as per SF2-22360 on 22/09/2025
        UNION ALL
        SELECT
            cm.source_system AS source_system,
            0 AS sequence_number,
            1 AS calculated_settlement_to_usd_rate,
            cm.movement_reference_source_id AS movement_reference_source_id,
            'DUMMY MOVEMENT' AS movement_reference,
            1 AS outstanding_original_ccy_to_settlement_ccy_rate,
            1 AS paid_original_ccy_to_settlement_ccy_rate,
            ce.exposure_opened_date AS movement_date,
            NULL AS to_date_outstanding_indemnity_in_original_ccy,
            NULL AS to_date_outstanding_fees_in_original_ccy,
            NULL AS to_date_outstanding_defence_in_original_ccy,
            0 AS movement_paid_indemnity_in_original_ccy,
            0 AS movement_paid_fees_in_original_ccy,
            0 AS movement_paid_defence_in_original_ccy,
            0 AS to_date_outstanding_indemnity_in_settlement_ccy,
            0 AS to_date_outstanding_fees_in_settlement_ccy,
            0 AS to_date_outstanding_defence_in_settlement_ccy,
            0 AS movement_paid_indemnity_in_settlement_ccy,
            0 AS movement_paid_fees_in_settlement_ccy, 
            0 AS movement_paid_defence_in_settlement_ccy,
            'N/A' AS movement_type,
            0 AS movement_outstanding_indemnity_in_original_ccy,
            0 AS movement_outstanding_fees_in_original_ccy,
            0 AS movement_outstanding_defence_in_original_ccy,
            0 AS movement_outstanding_indemnity_in_settlement_ccy,
            0 AS movement_outstanding_fees_in_settlement_ccy,
            0 AS movement_outstanding_defence_in_settlement_ccy,
            ce.claim_exposure_source_id AS exposure_reference, 
            ce.exposure_opened_date AS movement_creation_date, 
            1 AS does_not_erode_reserves,
            NULL AS indemnity_reserve_in_settlement_ccy,
            NULL AS fees_reserve_in_settlement_ccy,
            NULL AS defence_reserve_in_settlement_ccy,
            NULL AS payment_id,
            NULL AS payment_type,
            ce.signed AS signed_line,
            NULL AS transaction_tracking_status,
            NULL AS transfer_status,
            0 AS movement_net_payment_amount_in_original_ccy,
            0 AS movement_net_payment_amount_in_settlement_ccy,
            0 AS movement_tax_amount_in_original_ccy,
            0 AS movement_tax_amount_in_settlement_ccy,
            CASE
                WHEN (cm.settlement_currency IN ('EUR', 'USD', 'CAD', 'GBP')) THEN cm.settlement_currency
                ELSE 'USD'
            END AS settlement_currency,
            cm.settlement_currency as settlement_source_currency, --added new col as per SF2-22339
            IFNULL(cm.original_currency, 'USD') AS original_currency
        FROM UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSURE ce
        LEFT JOIN UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSUREEXTENSION cee
            ON (cee.claim_exposure_source_id = ce.claim_exposure_source_id)
           AND (cee.source_system = ce.source_system)
           AND (cee.is_active = 1) -- Added on 2025-08-11
        LEFT JOIN (SELECT
                       cm.source_system,
                       cm.claim_exposure_source_id,
                       cm.is_active,
                       cm.original_ccy_to_group_ccy_rate,
                       cm.audit_create_date_time,
                       cm.movement_reference_source_id,
                       cm.sequence_number,
                       cm.original_currency,
                       cm.settlement_currency,
                       cm.movement_period,
                       cm.movement_date,
                       cm.movement_type,
                       cm.hashbytes_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY
                               cm.claim_exposure_source_id 
                           ORDER BY
                               cm.source_system_event_date_time DESC,
                               cm.sequence_number ASC, -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                               cm.movement_reference_source_id ASC, -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                               cm.settlement_currency ASC, -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                               cm.original_currency ASC -- Added by Vojco K. on 2025-05-21 to enforce deterministic order
                           ) AS row_id
                       -- For exposures that have movements, some information should be taken from the first movement 
                       -- as in V9 are not pushed (marked as REMOVED)
                       -- This is because the old condition to filter the first movement was WHERE cm.sequence_number = 1
                       -- There are cases for which there are more than 1 line with sequence_number 1 which breaks ODS
                   FROM UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIMEXPOSUREMOVEMENT cm
                   WHERE (cm.source_system = 'ClaimCenter')
                    -- AND (cm.is_active = 1) (commented out as per original)
                     AND (cm.sequence_number = 1)) cm
            ON (cm.claim_exposure_source_id = ce.claim_exposure_source_id)
           AND (cm.source_system = ce.source_system)
           AND (cm.is_active = 1) -- Added on 2025-08-11
        INNER JOIN UNDRW_DEFAULT_020_STG.VW_BIDATACONTRACT_CLAIM c
            ON (c.claim_source_id = ce.claim_source_id)
           AND (c.source_system = ce.source_system)
           AND (c.is_active = 1) -- Added on 2025-08-11
        WHERE (ce.source_system = 'ClaimCenter')
          AND (ce.is_active = 1) -- Added on 2025-08-11
          AND (IFNULL(cee.scm_reference, '') = '')  -- we create DUMMY MOVEMENTS only for Non-SCM
          -- For exposures that have movements, some information should be taken from the first movement
          -- as in V9 are not pushed (marked as REMOVED)
          -- This is because the old condition to filter the first movement was WHERE cm.sequence_number = 1
          -- There are cases for which there are more than 1 line with sequence_number 1 which breaks ODS
          AND (IFNULL(cm.row_id, 1) = 1)
          -- All Non-SCM transactions from Migration (transfer_status = 'Migration') 
          -- and only completed ones from online (have transfer_status = 'Transaction Complete')
          AND (
               -- only exposures without movements -> for which the code should create DUMMY MOVEMENT
               (cm.movement_date IS NULL)
               OR
               -- exposures with movements -> only completed transactions
               ((cm.movement_date IS NOT NULL) AND (cm.movement_type <> 'SCM')
                -- AND (IFNULL(transfer_status, '') IN ('Migration', 'Transaction Complete'))  -- Commented for ticket: BI-4635
               )
              )
          AND (IFNULL(c.incident_report, 0) = 0)---Added filters as per SF2-22360 on 22/09/2025
          AND (IFNULL(c.is_retired, 0) = 0)---Added filters as per SF2-22360 on 22/09/2025
         ) AS t;

    -- *********************************************************************************************************
    --    Update sequence order - movements need to be shown in reverse chronological order in the front-end    
    -- *********************************************************************************************************
    UPDATE CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE AS cm
    SET cm.sequence_number_order_id = cm1.last_movement_group_sequence_id + 1 - cm.movement_group_sequence_id
    FROM (
        SELECT 
            movement_group_id,
            MAX(movement_group_sequence_id) AS last_movement_group_sequence_id
        FROM CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE
        GROUP BY movement_group_id
    ) cm1
    WHERE (cm.movement_group_id = cm1.movement_group_id);

    -- First update: Set rate to 1 where it's 0
    UPDATE CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE
    SET outstanding_original_ccy_to_settlement_ccy_rate = 1
    WHERE (outstanding_original_ccy_to_settlement_ccy_rate = 0);

    -- Second update: Set rate to 1 where it's 0
    UPDATE CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE
    SET paid_original_ccy_to_settlement_ccy_rate = 1
    WHERE (paid_original_ccy_to_settlement_ccy_rate = 0);

    -- Third update: Negate values for recovery movements
    UPDATE CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE
    SET 
        movement_paid_indemnity_in_settlement_ccy = -movement_paid_indemnity_in_settlement_ccy,
        movement_paid_indemnity_in_source_settlement_ccy = -movement_paid_indemnity_in_source_settlement_ccy, --added new col as per SF2-22339
        movement_paid_fees_in_settlement_ccy = -movement_paid_fees_in_settlement_ccy,
        movement_paid_fees_in_source_settlement_ccy = -movement_paid_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
        movement_paid_defence_in_settlement_ccy = -movement_paid_defence_in_settlement_ccy,
        movement_paid_defence_in_source_settlement_ccy = -movement_paid_defence_in_source_settlement_ccy, --added new col as per SF2-22339
        movement_paid_indemnity_in_original_ccy = -movement_paid_indemnity_in_original_ccy,  -- treated already when inserting into claim_movement
        movement_paid_fees_in_original_ccy = -movement_paid_fees_in_original_ccy,
        movement_paid_defence_in_original_ccy = -movement_paid_defence_in_original_ccy,
        movement_net_payment_amount_in_original_ccy = -movement_net_payment_amount_in_original_ccy,
        movement_net_payment_amount_in_settlement_ccy = -movement_net_payment_amount_in_settlement_ccy,
        movement_tax_amount_in_original_ccy = -movement_tax_amount_in_original_ccy,
        movement_tax_amount_in_settlement_ccy = -movement_tax_amount_in_settlement_ccy
    WHERE (movement_type = 'Recovery');

    UPDATE CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE AS cm_target
    SET 
        cm_target.to_date_outstanding_indemnity_in_original_ccy = cm_source.to_date_outstanding_indemnity_in_original_ccy,
        cm_target.to_date_outstanding_fees_in_original_ccy = cm_source.to_date_outstanding_fees_in_original_ccy,
        cm_target.to_date_outstanding_defence_in_original_ccy = cm_source.to_date_outstanding_defence_in_original_ccy,
        cm_target.to_date_outstanding_indemnity_in_settlement_ccy = cm_source.to_date_outstanding_indemnity_in_settlement_ccy,
        cm_target.to_date_outstanding_indemnity_in_source_settlement_ccy = cm_source.to_date_outstanding_indemnity_in_source_settlement_ccy,  --added new col as per SF2-22339
        cm_target.to_date_outstanding_fees_in_settlement_ccy = cm_source.to_date_outstanding_fees_in_settlement_ccy,
        cm_target.to_date_outstanding_fees_in_source_settlement_ccy = cm_source.to_date_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
        cm_target.to_date_outstanding_defence_in_settlement_ccy = cm_source.to_date_outstanding_defence_in_settlement_ccy,
        cm_target.to_date_outstanding_defence_in_source_settlement_ccy = cm_source.to_date_outstanding_defence_in_source_settlement_ccy  --added new col as per SF2-22339
    FROM (
        SELECT  
            x.exposure_reference,
            x.sequence_number,
            x.original_currency,
            x.settlement_currency,
            x.movement_reference,
            SUM(x.to_date_outstanding_indemnity_in_original_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_indemnity_in_original_ccy,
            SUM(x.to_date_outstanding_fees_in_original_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_fees_in_original_ccy,
            SUM(x.to_date_outstanding_defence_in_original_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_defence_in_original_ccy,
            SUM(x.to_date_outstanding_indemnity_in_settlement_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_indemnity_in_settlement_ccy,
            SUM(x.to_date_outstanding_indemnity_in_source_settlement_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_indemnity_in_source_settlement_ccy, --added new col as per SF2-22339
            SUM(x.to_date_outstanding_fees_in_settlement_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_fees_in_settlement_ccy,
            SUM(x.to_date_outstanding_fees_in_source_settlement_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
            SUM(x.to_date_outstanding_defence_in_settlement_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_defence_in_settlement_ccy,
            SUM(x.to_date_outstanding_defence_in_source_settlement_ccy) OVER (
                PARTITION BY x.exposure_reference, x.movement_group_id 
                ORDER BY x.movement_group_sequence_id
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS to_date_outstanding_defence_in_source_settlement_ccy   --added new col as per SF2-22339 
        FROM (
            SELECT
                cm.exposure_reference,
                cm.movement_group_id,
                cm.movement_group_sequence_id,
                cm.sequence_number,
                cm.original_currency,
                cm.settlement_currency,
                cm.movement_reference,
                LAG(cm.to_date_outstanding_indemnity_in_original_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_indemnity_in_original_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_indemnity_in_original_ccy
                    ELSE 0 
                END AS to_date_outstanding_indemnity_in_original_ccy,
                LAG(cm.to_date_outstanding_fees_in_original_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_fees_in_original_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_fees_in_original_ccy
                    ELSE 0 
                END AS to_date_outstanding_fees_in_original_ccy,
                LAG(cm.to_date_outstanding_defence_in_original_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_defence_in_original_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_defence_in_original_ccy
                    ELSE 0 
                END AS to_date_outstanding_defence_in_original_ccy,
                LAG(cm.to_date_outstanding_indemnity_in_settlement_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_indemnity_in_settlement_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_indemnity_in_settlement_ccy
                    ELSE 0 
                END AS to_date_outstanding_indemnity_in_settlement_ccy,
                LAG(cm.to_date_outstanding_indemnity_in_source_settlement_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_indemnity_in_source_settlement_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_indemnity_in_source_settlement_ccy
                    ELSE 0 
                END AS to_date_outstanding_indemnity_in_source_settlement_ccy,  --added new col as per SF2-22339
                LAG(cm.to_date_outstanding_fees_in_settlement_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_fees_in_settlement_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_fees_in_settlement_ccy
                    ELSE 0 
                END AS to_date_outstanding_fees_in_settlement_ccy,
                LAG(cm.to_date_outstanding_fees_in_source_settlement_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_fees_in_source_settlement_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_fees_in_source_settlement_ccy
                    ELSE 0 
                END AS to_date_outstanding_fees_in_source_settlement_ccy,  --added new col as per SF2-22339
                LAG(cm.to_date_outstanding_defence_in_settlement_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_defence_in_settlement_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_defence_in_settlement_ccy
                    ELSE 0 
                END AS to_date_outstanding_defence_in_settlement_ccy,
                LAG(cm.to_date_outstanding_defence_in_source_settlement_ccy, 1, 0) OVER (
                    PARTITION BY cm.exposure_reference, cm.movement_group_id -- Added by Vojco K. on 2025-05-20 to define the partition
                    ORDER BY cm.movement_group_sequence_id
                ) + cm.movement_outstanding_defence_in_source_settlement_ccy - CASE
                    WHEN (cm.movement_type = 'Payment') AND (cm.does_not_erode_reserves = 0) THEN cm.movement_paid_defence_in_source_settlement_ccy
                    ELSE 0 
                END AS to_date_outstanding_defence_in_source_settlement_ccy  --added new col as per SF2-22339
            FROM CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE cm
            WHERE (movement_type <> 'SCM')
        ) x
    ) cm_source
    WHERE (cm_target.exposure_reference = cm_source.exposure_reference)
      AND (cm_target.sequence_number = cm_source.sequence_number)
      AND (cm_target.original_currency = cm_source.original_currency)
      AND (cm_target.settlement_currency = cm_source.settlement_currency)
      AND (cm_target.movement_reference = cm_source.movement_reference);

    -- *********************************************************************************************
    --   Step 2: Merge the results to the final table
    -- *********************************************************************************************

    MERGE INTO CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT AS target
    USING (
        SELECT
            t.EXPOSURE_REFERENCE,
            t.SEQUENCE_NUMBER,
            t.MOVEMENT_REFERENCE,
            t.SETTLEMENT_CURRENCY,
            t.SETTLEMENT_SOURCE_CURRENCY, --added new col as per SF2-22339
            t.ORIGINAL_CURRENCY,
            t.MOVEMENT_GROUP_ID,
            t.MOVEMENT_GROUP_SEQUENCE_ID,
            t.SEQUENCE_NUMBER_ORDER_ID,
            t.OUTSTANDING_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            t.PAID_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            t.MOVEMENT_DATE,
            t.TO_DATE_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            t.TO_DATE_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            t.TO_DATE_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            t.TO_DATE_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            t.TO_DATE_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.TO_DATE_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            t.TO_DATE_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.TO_DATE_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            t.TO_DATE_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.MOVEMENT_PAID_INDEMNITY_IN_ORIGINAL_CCY,
            t.MOVEMENT_PAID_FEES_IN_ORIGINAL_CCY,
            t.MOVEMENT_PAID_DEFENCE_IN_ORIGINAL_CCY,
            t.MOVEMENT_PAID_INDEMNITY_IN_SETTLEMENT_CCY,
            t.MOVEMENT_PAID_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            t.MOVEMENT_PAID_FEES_IN_SETTLEMENT_CCY,
            t.MOVEMENT_PAID_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.MOVEMENT_PAID_DEFENCE_IN_SETTLEMENT_CCY,
            t.MOVEMENT_PAID_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.MOVEMENT_TYPE,
            t.MOVEMENT_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            t.MOVEMENT_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            t.MOVEMENT_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            t.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            t.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.MOVEMENT_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            t.MOVEMENT_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            t.MOVEMENT_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            t.MOVEMENT_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            t.DOES_NOT_ERODE_RESERVES,
            t.INDEMNITY_RESERVE_IN_SETTLEMENT_CCY,
            t.FEES_RESERVE_IN_SETTLEMENT_CCY,
            t.DEFENCE_RESERVE_IN_SETTLEMENT_CCY,
            t.PAYMENT_ID,
            t.PAYMENT_TYPE,
            t.SIGNED_LINE,
            t.TRANSACTION_TRACKING_STATUS,
            t.TRANSFER_STATUS,
            t.MOVEMENT_NET_PAYMENT_AMOUNT_IN_ORIGINAL_CCY,
            t.MOVEMENT_NET_PAYMENT_AMOUNT_IN_SETTLEMENT_CCY,
            t.MOVEMENT_TAX_AMOUNT_IN_ORIGINAL_CCY,
            t.MOVEMENT_TAX_AMOUNT_IN_SETTLEMENT_CCY,
            t.MOVEMENT_CREATION_DATE,
            SHA2_BINARY(
                IFNULL(t.EXPOSURE_REFERENCE, 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.SEQUENCE_NUMBER), 'NULL') || '#' ||
                IFNULL(t.MOVEMENT_REFERENCE, 'NULL') || '#' ||
                IFNULL(t.SETTLEMENT_CURRENCY, 'NULL') || '#' ||
                IFNULL(t.SETTLEMENT_SOURCE_CURRENCY, 'NULL') || '#' || --added new col as per SF2-22339
                IFNULL(t.ORIGINAL_CURRENCY, 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_GROUP_ID), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_GROUP_SEQUENCE_ID), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.SEQUENCE_NUMBER_ORDER_ID), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.OUTSTANDING_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.PAID_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_DATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_FEES_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||  --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_FEES_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||  --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.TO_DATE_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||  --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_INDEMNITY_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_FEES_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_DEFENCE_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_INDEMNITY_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' || --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_FEES_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_FEES_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||  --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_DEFENCE_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_PAID_DEFENCE_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||  --added new col as per SF2-22339
                IFNULL(t.MOVEMENT_TYPE, 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_FEES_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||   --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_FEES_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' ||  --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY), 'NULL') || '#' || --added new col as per SF2-22339
                IFNULL(TO_CHAR(t.DOES_NOT_ERODE_RESERVES), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.INDEMNITY_RESERVE_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.FEES_RESERVE_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.DEFENCE_RESERVE_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(t.PAYMENT_ID, 'NULL') || '#' ||
                IFNULL(t.PAYMENT_TYPE, 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.SIGNED_LINE), 'NULL') || '#' ||
                IFNULL(t.TRANSACTION_TRACKING_STATUS, 'NULL') || '#' ||
                IFNULL(t.TRANSFER_STATUS, 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_NET_PAYMENT_AMOUNT_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_NET_PAYMENT_AMOUNT_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_TAX_AMOUNT_IN_ORIGINAL_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_TAX_AMOUNT_IN_SETTLEMENT_CCY), 'NULL') || '#' ||
                IFNULL(TO_CHAR(t.MOVEMENT_CREATION_DATE, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM'), 'NULL')
            ) AS ROW_HASH_KEY,
            'STG' as CREATED_FROM,
            p.id as PROCESS_ID,
            'BI Data Contract' as SOURCE_NAME
        FROM CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE t
        CROSS JOIN CLAIMS_DEFAULT_000_COD.CLL_LATEST_RUN_ID p
        ) AS source
    ON (source.EXPOSURE_REFERENCE = target.EXPOSURE_REFERENCE AND
        source.SEQUENCE_NUMBER = target.SEQUENCE_NUMBER AND
        source.MOVEMENT_REFERENCE = target.MOVEMENT_REFERENCE AND
        source.SETTLEMENT_CURRENCY = target.SETTLEMENT_CURRENCY AND
        source.ORIGINAL_CURRENCY = target.ORIGINAL_CURRENCY)
    WHEN MATCHED AND target.ROW_HASH_KEY != source.ROW_HASH_KEY THEN
        UPDATE SET
            target.MOVEMENT_GROUP_ID = source.MOVEMENT_GROUP_ID,
            target.MOVEMENT_GROUP_SEQUENCE_ID = source.MOVEMENT_GROUP_SEQUENCE_ID,
            target.SEQUENCE_NUMBER_ORDER_ID = source.SEQUENCE_NUMBER_ORDER_ID,
            target.OUTSTANDING_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE = source.OUTSTANDING_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            target.PAID_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE = source.PAID_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            target.MOVEMENT_DATE = source.MOVEMENT_DATE,
            target.TO_DATE_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY = source.TO_DATE_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            target.TO_DATE_OUTSTANDING_FEES_IN_ORIGINAL_CCY = source.TO_DATE_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            target.TO_DATE_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY = source.TO_DATE_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            target.TO_DATE_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY = source.TO_DATE_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            target.TO_DATE_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY = source.TO_DATE_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            target.TO_DATE_OUTSTANDING_FEES_IN_SETTLEMENT_CCY = source.TO_DATE_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            target.TO_DATE_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY = source.TO_DATE_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            target.TO_DATE_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY = source.TO_DATE_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            target.TO_DATE_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY = source.TO_DATE_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            target.MOVEMENT_PAID_INDEMNITY_IN_ORIGINAL_CCY = source.MOVEMENT_PAID_INDEMNITY_IN_ORIGINAL_CCY,
            target.MOVEMENT_PAID_FEES_IN_ORIGINAL_CCY = source.MOVEMENT_PAID_FEES_IN_ORIGINAL_CCY,
            target.MOVEMENT_PAID_DEFENCE_IN_ORIGINAL_CCY = source.MOVEMENT_PAID_DEFENCE_IN_ORIGINAL_CCY,
            target.MOVEMENT_PAID_INDEMNITY_IN_SETTLEMENT_CCY = source.MOVEMENT_PAID_INDEMNITY_IN_SETTLEMENT_CCY,
            target.MOVEMENT_PAID_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY = source.MOVEMENT_PAID_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            target.MOVEMENT_PAID_FEES_IN_SETTLEMENT_CCY = source.MOVEMENT_PAID_FEES_IN_SETTLEMENT_CCY,
            target.MOVEMENT_PAID_FEES_IN_SOURCE_SETTLEMENT_CCY = source.MOVEMENT_PAID_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            target.MOVEMENT_PAID_DEFENCE_IN_SETTLEMENT_CCY = source.MOVEMENT_PAID_DEFENCE_IN_SETTLEMENT_CCY,
            target.MOVEMENT_PAID_DEFENCE_IN_SOURCE_SETTLEMENT_CCY = source.MOVEMENT_PAID_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            target.MOVEMENT_TYPE = source.MOVEMENT_TYPE,
            target.MOVEMENT_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY = source.MOVEMENT_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            target.MOVEMENT_OUTSTANDING_FEES_IN_ORIGINAL_CCY = source.MOVEMENT_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            target.MOVEMENT_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY = source.MOVEMENT_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            target.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY = source.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            target.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY = source.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            target.MOVEMENT_OUTSTANDING_FEES_IN_SETTLEMENT_CCY = source.MOVEMENT_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            target.MOVEMENT_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY = source.MOVEMENT_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            target.MOVEMENT_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY = source.MOVEMENT_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            target.MOVEMENT_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY = source.MOVEMENT_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            target.DOES_NOT_ERODE_RESERVES = source.DOES_NOT_ERODE_RESERVES,
            target.INDEMNITY_RESERVE_IN_SETTLEMENT_CCY = source.INDEMNITY_RESERVE_IN_SETTLEMENT_CCY,
            target.FEES_RESERVE_IN_SETTLEMENT_CCY = source.FEES_RESERVE_IN_SETTLEMENT_CCY,
            target.DEFENCE_RESERVE_IN_SETTLEMENT_CCY = source.DEFENCE_RESERVE_IN_SETTLEMENT_CCY,
            target.PAYMENT_ID = source.PAYMENT_ID,
            target.PAYMENT_TYPE = source.PAYMENT_TYPE,
            target.SIGNED_LINE = source.SIGNED_LINE,
            target.TRANSACTION_TRACKING_STATUS = source.TRANSACTION_TRACKING_STATUS,
            target.TRANSFER_STATUS = source.TRANSFER_STATUS,
            target.MOVEMENT_NET_PAYMENT_AMOUNT_IN_ORIGINAL_CCY = source.MOVEMENT_NET_PAYMENT_AMOUNT_IN_ORIGINAL_CCY,
            target.MOVEMENT_NET_PAYMENT_AMOUNT_IN_SETTLEMENT_CCY = source.MOVEMENT_NET_PAYMENT_AMOUNT_IN_SETTLEMENT_CCY,
            target.MOVEMENT_TAX_AMOUNT_IN_ORIGINAL_CCY = source.MOVEMENT_TAX_AMOUNT_IN_ORIGINAL_CCY,
            target.MOVEMENT_TAX_AMOUNT_IN_SETTLEMENT_CCY = source.MOVEMENT_TAX_AMOUNT_IN_SETTLEMENT_CCY,
            target.MOVEMENT_CREATION_DATE = source.MOVEMENT_CREATION_DATE,
            target.ROW_HASH_KEY = source.ROW_HASH_KEY,
            target.PROCESS_ID = source.PROCESS_ID,
            target.SOURCE_NAME = source.SOURCE_NAME
    WHEN NOT MATCHED THEN
        INSERT (
            EXPOSURE_REFERENCE,
            SEQUENCE_NUMBER,
            MOVEMENT_REFERENCE,
            SETTLEMENT_CURRENCY,
            SETTLEMENT_SOURCE_CURRENCY, --added new col as per SF2-22339
            ORIGINAL_CURRENCY,
            MOVEMENT_GROUP_ID,
            MOVEMENT_GROUP_SEQUENCE_ID,
            SEQUENCE_NUMBER_ORDER_ID,
            OUTSTANDING_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            PAID_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            MOVEMENT_DATE,
            TO_DATE_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            TO_DATE_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            TO_DATE_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            TO_DATE_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            TO_DATE_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            TO_DATE_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            TO_DATE_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            TO_DATE_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            TO_DATE_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            MOVEMENT_PAID_INDEMNITY_IN_ORIGINAL_CCY,
            MOVEMENT_PAID_FEES_IN_ORIGINAL_CCY,
            MOVEMENT_PAID_DEFENCE_IN_ORIGINAL_CCY,
            MOVEMENT_PAID_INDEMNITY_IN_SETTLEMENT_CCY,
            MOVEMENT_PAID_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            MOVEMENT_PAID_FEES_IN_SETTLEMENT_CCY,
            MOVEMENT_PAID_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            MOVEMENT_PAID_DEFENCE_IN_SETTLEMENT_CCY,
            MOVEMENT_PAID_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            MOVEMENT_TYPE,
            MOVEMENT_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            MOVEMENT_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            MOVEMENT_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            MOVEMENT_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            MOVEMENT_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            MOVEMENT_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            MOVEMENT_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            MOVEMENT_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            MOVEMENT_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            DOES_NOT_ERODE_RESERVES,
            INDEMNITY_RESERVE_IN_SETTLEMENT_CCY,
            FEES_RESERVE_IN_SETTLEMENT_CCY,
            DEFENCE_RESERVE_IN_SETTLEMENT_CCY,
            PAYMENT_ID,
            PAYMENT_TYPE,
            SIGNED_LINE,
            TRANSACTION_TRACKING_STATUS,
            TRANSFER_STATUS,
            MOVEMENT_NET_PAYMENT_AMOUNT_IN_ORIGINAL_CCY,
            MOVEMENT_NET_PAYMENT_AMOUNT_IN_SETTLEMENT_CCY,
            MOVEMENT_TAX_AMOUNT_IN_ORIGINAL_CCY,
            MOVEMENT_TAX_AMOUNT_IN_SETTLEMENT_CCY,
            MOVEMENT_CREATION_DATE,
            ROW_HASH_KEY,
            CREATED_AT_TS,
            CREATED_BY,
            CREATED_FROM,
            PROCESS_ID,
            SOURCE_NAME)
        VALUES (
            source.EXPOSURE_REFERENCE,
            source.SEQUENCE_NUMBER,
            source.MOVEMENT_REFERENCE,
            source.SETTLEMENT_CURRENCY,
            source.SETTLEMENT_SOURCE_CURRENCY, --added new col as per SF2-22339
            source.ORIGINAL_CURRENCY,
            source.MOVEMENT_GROUP_ID,
            source.MOVEMENT_GROUP_SEQUENCE_ID,
            source.SEQUENCE_NUMBER_ORDER_ID,
            source.OUTSTANDING_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            source.PAID_ORIGINAL_CCY_TO_SETTLEMENT_CCY_RATE,
            source.MOVEMENT_DATE,
            source.TO_DATE_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            source.TO_DATE_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            source.TO_DATE_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            source.TO_DATE_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            source.TO_DATE_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            source.TO_DATE_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            source.TO_DATE_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            source.TO_DATE_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            source.TO_DATE_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            source.MOVEMENT_PAID_INDEMNITY_IN_ORIGINAL_CCY,
            source.MOVEMENT_PAID_FEES_IN_ORIGINAL_CCY,
            source.MOVEMENT_PAID_DEFENCE_IN_ORIGINAL_CCY,
            source.MOVEMENT_PAID_INDEMNITY_IN_SETTLEMENT_CCY,
            source.MOVEMENT_PAID_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            source.MOVEMENT_PAID_FEES_IN_SETTLEMENT_CCY,
            source.MOVEMENT_PAID_FEES_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            source.MOVEMENT_PAID_DEFENCE_IN_SETTLEMENT_CCY,
            source.MOVEMENT_PAID_DEFENCE_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            source.MOVEMENT_TYPE,
            source.MOVEMENT_OUTSTANDING_INDEMNITY_IN_ORIGINAL_CCY,
            source.MOVEMENT_OUTSTANDING_FEES_IN_ORIGINAL_CCY,
            source.MOVEMENT_OUTSTANDING_DEFENCE_IN_ORIGINAL_CCY,
            source.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SETTLEMENT_CCY,
            source.MOVEMENT_OUTSTANDING_INDEMNITY_IN_SOURCE_SETTLEMENT_CCY,  --added new col as per SF2-22339
            source.MOVEMENT_OUTSTANDING_FEES_IN_SETTLEMENT_CCY,
            source.MOVEMENT_OUTSTANDING_FEES_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            source.MOVEMENT_OUTSTANDING_DEFENCE_IN_SETTLEMENT_CCY,
            source.MOVEMENT_OUTSTANDING_DEFENCE_IN_SOURCE_SETTLEMENT_CCY, --added new col as per SF2-22339
            source.DOES_NOT_ERODE_RESERVES,
            source.INDEMNITY_RESERVE_IN_SETTLEMENT_CCY,
            source.FEES_RESERVE_IN_SETTLEMENT_CCY,
            source.DEFENCE_RESERVE_IN_SETTLEMENT_CCY,
            source.PAYMENT_ID,
            source.PAYMENT_TYPE,
            source.SIGNED_LINE,
            source.TRANSACTION_TRACKING_STATUS,
            source.TRANSFER_STATUS,
            source.MOVEMENT_NET_PAYMENT_AMOUNT_IN_ORIGINAL_CCY,
            source.MOVEMENT_NET_PAYMENT_AMOUNT_IN_SETTLEMENT_CCY,
            source.MOVEMENT_TAX_AMOUNT_IN_ORIGINAL_CCY,
            source.MOVEMENT_TAX_AMOUNT_IN_SETTLEMENT_CCY,
            source.MOVEMENT_CREATION_DATE,
            source.ROW_HASH_KEY,
            CURRENT_TIMESTAMP::TIMESTAMP_TZ,
            CURRENT_ROLE(),
            source.CREATED_FROM,
            source.PROCESS_ID,
            source.SOURCE_NAME);

    DELETE FROM CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT AS target
    WHERE NOT EXISTS (
        SELECT 1
        FROM CLAIMS_DEFAULT.CLL_WT_CLAIM_MOVEMENT_STAGE AS source
        WHERE (target.EXPOSURE_REFERENCE = source.EXPOSURE_REFERENCE)
          AND (target.SEQUENCE_NUMBER = source.SEQUENCE_NUMBER)
          AND (target.MOVEMENT_REFERENCE = source.MOVEMENT_REFERENCE)
          AND (target.SETTLEMENT_CURRENCY = source.SETTLEMENT_CURRENCY)
          AND (target.ORIGINAL_CURRENCY = source.ORIGINAL_CURRENCY)
    );

    -- *********************************************************************************************
    --   Finish
    -- *********************************************************************************************

    RETURN ('Procedure executed.');

    EXCEPTION
        WHEN EXPRESSION_ERROR THEN
            ROLLBACK;
            RAISE;
        WHEN STATEMENT_ERROR THEN
            ROLLBACK;
            RAISE;
        WHEN OTHER THEN
            ROLLBACK;
            RAISE;

END;
$$;