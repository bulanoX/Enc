CREATE PROCEDURE [Staging].[usp_LoadPolicy_All_delta]
AS
BEGIN

	DECLARE @LastAuditDate DATETIME2(7)

	SELECT 
		@LastAuditDate = MAX(ISNULL(AuditModifyDateTime,AuditCreateDateTime) )
	FROM Staging.Policy

	SET @LastAuditDate = ISNULL(@LastAuditDate, '1900-01-01')
	
	TRUNCATE TABLE Staging.Policy_All

	INSERT INTO Staging.Policy_All
	(
		 PolicyReference							
		,SourceSystem								
		,SourceSystemRiskId							
		,AdditionalInsuredParty						
		,InsuredAddress								
		,InsuredCity								
		,InsuredState								
		,InsuredCountry								
		,InsuredPostCode							
		,IsAdmitted									
		,AffinityIdentifier 						
		,AgressoBrokerID							
		,AnnualRevenues								
		,AttachmentPoint							
		,BrokerAccessPolicyNumber					
		,BrokerNetworkName							
		,CapitaBrokerID								
		,Cedant										
		,Coinsurance								
		,ContractBasis								
		,CoverholderName							
		,EarliestRationaleQuestionnaireDate			
		,ErpExpirationDate							
		,FundType									
		,InitiativeUS								
		,InsuranceType								
		,InsuredName	
		 ,InsuredLegalTradingName 
		,BrokerAccessReferralFlag					
		,IsNewYorkFreeTradeZone						
		,IsQuote									
		,IsBoundQuote								
		,ExpiringPolicyReference				
		,EndorsementNumber
		,MarketSegment								
		,MarketSegmentCode							
		,McareAssessment							
		,MethodOfPlacement							
		,MethodOfPlacementCode						
		,NetworkEndDate								
		,NetworkStartDate							
		,BrokerNoticeOfClaim						
		,PatientCompensationFund					
		,PlacingBrokerContact						
		,PlacingBrokerNumber
		,PlacingBrokerSourceId
		,PolicyReferenceMOPCode						
		,PolicyReferenceMOPCodeRiskNumber			
		,PolicyReferenceRiskNumber					
		,PolicyReferenceYOA
		,PolicyType
		,PolicyURL									
		,PolicyURLLabel								
		,ProducingBrokerContact						
		,ReinsuredParty		
		,RiskType
		,RunOffDate		
		,ServiceCompanyLocation
		,BrokerServiceOfSuit						
		,UPIFlag									
		,UniqueMarketReference						
		,YearOfAccount								
		,HashBytesId								
		,ProducingBrokerBranchName					
		,ProducingBrokerSourceId					
		,PlacingBrokerBranchName					
		,CRMBrokerName		
		,DomicileCountry
		,IsRenewal					
		,ProducingBrokerNumber						
		,BrokerBeazleyTradeId						
		,LimitCCYToSettlementCCYRateCurrent			
		,LimitCCYToSettlementCCYRateMelded			
		,PriorityDelete								
		,SourcesystemKey	
		,TaxPercentage
		,TriaPercentage								
		,OriginatingSourceSystem
		,USMiscMedLifeSciencesPolicyCOBCode
		,LargeRiskExemption
		,LeadFeesPercentage
	)
	SELECT DISTINCT

		 PolicyReference								= p.PolicyReference
		,SourceSystem									= p.SourceSystem
		,SourceSystemRiskId								= pe.SourceSystemRiskId
		,AdditionalInsuredParty							= pe.AdditionalInsuredParty
		,InsuredAddress									= pe.InsuredAddress
		,InsuredCity									=  p.InsuredCity
		,InsuredState									=  p.InsuredState
		,InsuredCountry									=  p.InsuredCountry
		,InsuredPostCode								= pe.InsuredPostCode
		,IsAdmitted										=  CASE
																WHEN p.SourceSystem = 'StagingDataContract' THEN 'Lloyds'
																ELSE p.IsAdmitted
															END
		,AffinityIdentifier 							= pe.AffinityIdentifier 
		,AgressoBrokerID								= pe.AgressoBrokerID
		,AnnualRevenues									= pe.AnnualRevenues
		,AttachmentPoint								= pe.AttachmentPoint
		,BrokerAccessPolicyNumber						= pe.BrokerAccessPolicyNumber
		,BrokerNetworkName								= pe.BrokerNetworkName
		,CapitaBrokerID									= pe.CapitaBrokerID
		,Cedant											= pe.Cedant								
		,Coinsurance									= pe.Coinsurance
		,ContractBasis									= pe.ContractBasis
		,CoverholderName								= pe.CoverholderName						
		,EarliestRationaleQuestionnaireDate				= se.EarliestRationaleQuestionnaireDate
		,ErpExpirationDate								= CASE WHEN p.SourceSystem = 'GameChanger' THEN se.ErpExpirationDate ELSE pe.ErpExpirationDate END
		,FundType										= pe.FundType								
		,InitiativeUS									= pe.InitiativeUS							
		,InsuranceType									= pe.InsuranceType							
		,InsuredName									=  p.InsuredName	
		 ,InsuredLegalTradingName						= p.InsuredLegalTradingName 
		,BrokerAccessReferralFlag						= pe.BrokerAccessReferralFlag				
		,IsNewYorkFreeTradeZone							= pe.IsNewYorkFreeTradeZone				
		,IsQuote										=  p.IsQuote
		,IsBoundQuote									=  p.IsBoundQuote
		,ExpiringPolicyReference						=  p.ExpiringPolicyReference	
		,EndorsementNumber								= pe.EndorsementNumber
		,MarketSegment									=  CASE 
																WHEN p.SourceSystem = 'StagingDataContract' THEN
																												CASE p.MarketSegment
																												               WHEN 'PE' THEN 'Private Enterprise'
																												               WHEN 'MM' THEN 'Mid-Market'
																												END
																--WHEN p.SourceSystem = 'BeazleyPro' THEN NULL
																ELSE p.MarketSegment
															END
		,MarketSegmentCode								=  p.MarketSegmentCode
		,McareAssessment								= pe.McareAssessment
		,MethodOfPlacement								= CASE 
																WHEN p.SourceSystem IN ('BeazleyPro', 'StagingDataContract') THEN mp.[Name]
																ELSE ISNULL(mp.[Name], pe.MethodOfPlacement)
															END
		,MethodOfPlacementCode							= CASE 
																WHEN p.SourceSystem = 'StagingDataContract' THEN 
																                                                CASE p.IsAdmitted
																                                                              WHEN '1' THEN 'V' --This is the MOP code we use for BICI
																                                                              ELSE 'W' --This is the MOP code we use for internal decs
																                                                END
																ELSE pe.MethodOfPlacementCode
															END
		,NetworkEndDate									= pe.NetworkEndDate
		,NetworkStartDate								= pe.NetworkStartDate
		,BrokerNoticeOfClaim							= pe.BrokerNoticeOfClaim
		,PatientCompensationFund						= pe.PatientCompensationFund
		,PlacingBrokerContact							=  p.PlacingBrokerContact
		,PlacingBrokerNumber 							=  p.PlacingBrokerNumber
		,PlacingBrokerSourceId							=  p.PlacingBrokerSourceId
		,PolicyReferenceMOPCode							= pe.PolicyReferenceMOPCode
		,PolicyReferenceMOPCodeRiskNumber				= pe.PolicyReferenceMOPCodeRiskNumber
		,PolicyReferenceRiskNumber						= pe.PolicyReferenceRiskNumber
		,PolicyReferenceYOA								= pe.PolicyReferenceYOA
		,PolicyType										= pe.PolicyType
		,PolicyURL										= pe.PolicyURL
		,PolicyURLLabel									= pe.PolicyURLLabel
		,ProducingBrokerContact							= pe.ProducingBrokerContact
		,ReinsuredParty									= pe.ReinsuredParty
		,RiskType                                       = pe.RiskType
		,RunOffDate										= pe.RunOffDate
		,ServiceCompanyLocation							= pe.ServiceCompanyLocation
		,BrokerServiceOfSuit							= pe.BrokerServiceOfSuit
		,UPIFlag										= CASE 
																WHEN p.SourceSystem = 'BeazleyPro' THEN ISNULL(pe.UPIFlag, 0)
																ELSE 0
															END
		,UniqueMarketReference							=  p.UniqueMarketReference
		,YearOfAccount									=  p.YearOfAccount
		,HashBytesId									=  p.HashBytesId
		,ProducingBrokerBranchName						= pe.ProducingBrokerBranchName
		,ProducingBrokerSourceId						= pe.ProducingBrokerSourceId
		,PlacingBrokerBranchName						=  p.PlacingBrokerBranchName
		,CRMBrokerName									= pe.CRMBrokerName
		,DomicileCountry                                = pe.DomicileCountry
		,IsRenewal										=  CASE 
																WHEN p.SourceSystem = 'BeazleyPro' THEN 
																                                       CASE 
																                                                 WHEN p.ExpiringPolicyReference IS NOT NULL THEN 1 
																                                                 ELSE NULL
																                                       END
																WHEN p.SourceSystem = 'StagingDataContract' THEN NULL
																ELSE p.IsRenewal
															END

		,ProducingBrokerNumber							=  pe.ProducingBrokerNumber
		,BrokerBeazleyTradeId							=  BrokerBeazleyTradeId		
		,LimitCCYToSettlementCCYRateCurrent				= se.LimitCCYToSettlementCCYRateCurrent
		,LimitCCYToSettlementCCYRateMelded				= se.LimitCCYToSettlementCCYRateMelded
		,PriorityDelete									= CASE 
																WHEN p.SourceSystem IN ('Eurobase', 'Unirisx', 'Gamechanger', 'CIPS', 'myBeazley', 'US High Value Homeowners', 'USHVH','BeazleyPro') THEN 1
																WHEN p.SourceSystem = 'FDR' THEN 2
																ELSE 3
															END
		,SourcesystemKey								= pe.SourcesystemKey
		,TaxPercentage									= pe.TaxPercentage
		,TriaPercentage									= CASE
																WHEN p.SourceSystem = 'BeazleyPro' THEN pe.TriaPercentage
																ELSE NULL
															END
		,pe.OriginatingSourceSystem
		,pe.USMiscMedLifeSciencesPolicyCOBCode
		,LargeRiskExemption								= pe.LargeRiskExemption
		,LeadFeesPercentage								= pe.LeadFeesPercentage
		

	FROM BeazleyIntelligenceDataContract.Outbound.vw_Policy p with (nolock)

	INNER JOIN BeazleyIntelligenceDataContract.Outbound.vw_PolicyExtension  pe with (nolock) 
	ON p.SourceSystem = pe.SourceSystem 
	AND p.PolicySourceId = pe.PolicySourceId 
	
	LEFT JOIN BeazleyIntelligenceDataContract.Outbound.vw_SectionExtension  se with (nolock) 
	ON p.SourceSystem = se.SourceSystem
	AND p.PolicySourceId = se.PolicySourceId
	AND se.SectionSequenceId = 1

	LEFT JOIN Staging_MDS.MDS_Staging.MethodOfPlacement mp 
	ON mp.Code =  CASE WHEN p.SourceSystem = 'StagingDataContract' 
							THEN CASE p.IsAdmitted
									 WHEN '1' THEN 'V' --This is the MOP code we use for BICI
									 ELSE 'W' --This is the MOP code we use for internal decs
								END
					 ELSE pe.MethodOfPlacementCode 
				END
 
	LEFT JOIN Staging.Policy sp on  sp.PolicyReference = p.PolicyReference



    LEFT JOIN (
			SELECT			PolicyReference, 
							SectionReference, 
							LondonRefNo, 
							NoOfApp		=	ROW_NUMBER() over(partition by PolicyReference order by SectionReference, LondonRefNo)
			FROM			Staging.EtrekUnirisxDedupe
   )ep on  ep.PolicyReference = p.PolicyReference
		AND ep.NoOfApp = 1

	WHERE ISNULL(pe.OriginatingSourceSystem, 'N\A') NOT IN ('FDR','USHVH', 'US High Value Homeowners') 

	AND (
		(
			ISNULL(p.AuditModifyDatetime, p.AuditCreateDateTime) >= @LastAuditDate
			OR
			ISNULL(pe.AuditModifyDatetime, pe.AuditCreateDateTime) >= @LastAuditDate
			OR
			ISNULL(se.AuditModifyDatetime, se.AuditCreateDateTime) >= @LastAuditDate
		
		)
	
		OR 
			(
			sp.PolicyReference IS NULL
			AND ep.PolicyReference IS NULL
			)
			)


 --   UPDATE s
	--SET PlacingBrokerNumber = p.PlacingBrokerNumber
	--FROM Staging.Policy_All s with (nolock) 
	--     INNER JOIN  
	--	 (SELECT PolicyReference
	--			,PlacingBrokerNumber = 2000000 + CASE  WHEN p.PlacingBrokerNumber IS NOT NULL
	--										 THEN p.PlacingBrokerNumber 
	--										 ELSE RANK() OVER (ORDER BY Utility.udf_ProcessString(UPPER(p.PlacingBrokerBranchName), 1) )  
	--									END

	--	 FROM BeazleyIntelligenceDataContract.Outbound.vw_Policy p with (nolock)
	--	 WHERE 
	--		ISNULL(p.PlacingBrokerBranchName, '') <> ''
	--		AND p.PlacingBrokerBranchName <> 'N/A'
	--		AND p.SourceSystem ='Unirisx' 
	--		)p
	--	 ON s.PolicyReference = p.PolicyReference
	--WHERE 
	--s.SourceSystem ='Unirisx'	
	
	--UPDATE s
	--SET ProducingBrokerNumber = pe.ProducingBrokerNumber
	--FROM Staging.Policy_All s with (nolock) 
	--     INNER JOIN  
	--	 (SELECT PolicyReference
	--			,ProducingBrokerNumber = 3000000 + CASE WHEN  pe.ProducingBrokerNumber IS NOT NULL
	--										 THEN pe.ProducingBrokerNumber 
	--										 ELSE RANK() OVER (ORDER BY Utility.udf_ProcessString(UPPER(pe.ProducingBrokerBranchName), 1) )  
	--									END

	--	 FROM BeazleyIntelligenceDataContract.Outbound.vw_PolicyExtension pe with (nolock)
	--	 WHERE 
	--		ISNULL(pe.ProducingBrokerBranchName, '') <> ''
	--		AND pe.ProducingBrokerBranchName <> 'N/A'
	--		AND pe.SourceSystem ='Unirisx' 
	--		)pe
	--	 ON s.PolicyReference = pe.PolicyReference
	--WHERE 
	--s.SourceSystem ='Unirisx'	

			,PriorityDelete									= CASE 
																WHEN p.SourceSystem IN ('Eurobase', 'Unirisx', 'Gamechanger', 'CIPS', 'myBeazley', 'US High Value Homeowners', 'USHVH','BeazleyPro') THEN 1
																WHEN p.SourceSystem = 'FDR' THEN 2
																ELSE 3

	DELETE
	FROM Staging.Policy_All
	WHERE OriginatingSourceSystem = 'EazyPro' 
	AND YearOfAccount >= 2017

	IF (SELECT MAX(AuditCreateDateTime) FROM Staging.Policy) IS NULL
		DELETE t
		FROM
		(
			SELECT 
				PolicyReference, 
				SourceSystem, 
				PriorityDelete,
				ROW_NUMBER() OVER (PARTITION BY PolicyReference ORDER BY PriorityDelete) AS RowNo
			FROM staging.policy_all WITH(NOLOCK)
		
		) AS t
		WHERE RowNo > 1
	

	DELETE pa
	FROM staging.Policy_All pa
	INNER JOIN staging.Policy_All p
	ON p.PolicyReference = pa.PolicyReference

	WHERE 
		(pa.SourceSystem = 'FDR' and p.SourceSystem = 'BeazleyPro')
		OR
		(pa.SourceSystem = 'StagingDataContract' and p.SourceSystem = 'Unirisx')


	DELETE pa
	FROM staging.Policy_All pa
	INNER JOIN staging.Policy p
	ON p.PolicyReference = pa.PolicyReference

	WHERE 
		(pa.SourceSystem = 'FDR' and p.SourceSystem = 'BeazleyPro')
		OR
		(pa.SourceSystem = 'StagingDataContract' and p.SourceSystem = 'Unirisx')

	
END