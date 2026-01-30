CREATE PROCEDURE Staging.usp_LoadBICICessionMultiplier
AS
BEGIN

	TRUNCATE TABLE Staging.BICICessionMultiplier

	INSERT INTO Staging.BICICessionMultiplier
	(
		AgressoReference
		,CessionMultiplier
	)
	SELECT
		AgressoReference            = x.PolicyReference
		,CessionMultiplier          = x.Cession / NULLIF(x.Premium, 0)

	FROM
		(
			SELECT
				PolicyReference         = pt.Reference
				,Premium                = SUM(pt.GPW * -1)
				,Cession                = SUM(ISNULL(pt.ICWP_F, 0) + ISNULL(pt.ICWP_Q, 0) + ISNULL(pt.ICWP_S, 0) + ISNULL(pt.ICWP_EMBQ,0))
			FROM [Staging_BICI_AccessDB].[BICI_AccessDB_Staging].[TBL:PremiumTable] pt
			WHERE Period2 = (SELECT LatestPeriod = MAX(Period2) FROM [Staging_BICI_AccessDB].[BICI_AccessDB_Staging].[TBL:PremiumTable] pt)
			GROUP BY pt.Reference
		) x

	WHERE Premium <> 0
	AND Cession <> 0

	UNION ALL

	SELECT
		AgressoReference            = x.PolicyReference
		,CessionMultiplier          = x.Cession / NULLIF(x.Premium, 0)
	FROM
		(
			SELECT
			PolicyReference         = mpct.Reference
			,Premium                = SUM(mpct.GrossWrittenPremium)
			,Cession                = SUM(ISNULL(mpct.CededPremiumToInternalQuotaShareTreaty, 0) + ISNULL(mpct.CededPremiumToInternalFAC, 0))
			FROM [Staging_BICI_AccessDB].[BICI_AccessDB_Staging].[TBL:MarinePremCommTable] mpct
			WHERE Period2 = (SELECT LatestPeriod = MAX(Period2) FROM  [Staging_BICI_AccessDB].[BICI_AccessDB_Staging].[TBL:MarinePremCommTable])
			GROUP BY mpct.Reference
		) x

	WHERE Premium <> 0
	AND Cession <> 0

	UNION ALL

	SELECT
		AgressoReference            = x.PolicyReference
		,CessionMultiplier          = x.Cession / NULLIF(x.Premium, 0)
	FROM
		(
			SELECT
				PolicyReference         = wpct.Reference
				,Premium                = SUM(wpct.GrossWrittenPremium)
				,Cession                = SUM(ISNULL(wpct.CededPremiumToInternalQuotaShareTreaty, 0) + ISNULL(wpct.CededPremiumToInternalSurplusTreaty, 0))
			FROM [Staging_BICI_AccessDB].[BICI_AccessDB_Staging].[TBL:WeatherPremCommTable] wpct
			WHERE Period2 = (SELECT LatestPeriod = MAX(Period2) FROM  [Staging_BICI_AccessDB].[BICI_AccessDB_Staging].[TBL:WeatherPremCommTable])
			GROUP BY wpct.Reference
		) x
	WHERE Premium <> 0
	AND Cession <> 0


END