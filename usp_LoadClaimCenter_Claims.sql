CREATE PROCEDURE [Staging].[usp_LoadClaimCenter_Claims]
AS
BEGIN

	--DROP TABLE IF EXISTS Staging.ClaimCenter_Claims
	TRUNCATE TABLE Staging.ClaimCenter_Claims

	INSERT INTO Staging.ClaimCenter_Claims
	(
		PolicyNumber
		,BinderReference
	)
	SELECT 
		DISTINCT 
			c.PolicyNumber
			, ce.BinderReference
		--INTO Staging.ClaimCenter_Claims
	FROM BeazleyIntelligenceDataContract.Outbound.vw_ClaimExposure ce
						
	INNER MERGE JOIN BeazleyIntelligenceDataContract.Outbound.vw_Claim c ON ce.SourceSystem = c.SourceSystem AND ce.ClaimSourceId = c.ClaimSourceId
						
	WHERE ce.SourceSystem = 'claimcenter'
		--AND ce.IsActive = 1
		--AND c.IsActive = 1
		AND BinderReference IS NOT NULL		
		AND ISNULL(c.IsRetired, 0) = 0	

END