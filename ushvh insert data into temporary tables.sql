
TRUNCATE TABLE ##HVHPolicy
TRUNCATE TABLE BeazleyIntelligenceExtractor.USHVH.HVHSection  -- Replace ##HVHSection with USHVH.HVHSection aspart of improvement ticket BI-7943
TRUNCATE TABLE ##HVHtransaction

-- ////////////////////////////////////////////////////////////////////
-- DELETE DUFF ROWS
DELETE ##HVHUS WHERE PK_policy IS NULL AND (pk_section IS NULL OR pk_section = '')   --  
DELETE ##HVHUS WHERE NULLIF(coveragename,'') IS NULL AND NULLIF(Trifocus,'') IS NULL -- Remove incomplete section rows

-- REMOVE ALL BUT LATEST POLICY ROW WHERE ROWS EXIST IN MULTIPLE FILES.  TEMP MEASURE... SHOULD NOT HAPPEN WHEN WE RECEIVE JUST ONE FILE
DELETE A
-- SELECT *
FROM ##HVHUS A
INNER JOIN (SELECT policyreference, pk_policy = MAX(pk_policy)
			FROM ##HVHUS
			GROUP BY policyreference) B
ON A.policyReference = B.policyreference 
AND a.pk_policy <> b.pk_policy
 

-- ////////////////////////////////////////////////////////////////////
--GET BINDER REFS
INSERT ##HVHBinderMapping 
(YOA, BinderReference)
SELECT  
	   FK_YOA
      ,[SectionReference]
FROM [BeazleyIntelligenceDataSets].[DataContract].[Section]
WHERE section.SectionReference LIKE 'B6188C%'
AND  (section.SectionReferenceCOBCode = 'YH' OR section.SectionReferenceCOBCode = '3Y')

-- ///////////////////////////////////////////////////////////////////
-- IDENTIFY LATEST POLICY/SECTION RECORD WHERE MULTIPLE EXISTS

IF (OBJECT_ID('tempdb..#LatestRecord') IS NOT NULL) 
DROP TABLE #LatestRecord
SELECT FK_Policy, SectionSequenceID, PK_Section = MAX(PK_Section)
INTO #LatestRecord
FROM ##HVHUS
GROUP BY FK_Policy , SectionSequenceID
 
-- ////////////////////////////////////////////////////////////////////
-- POLICY INSERT 
-- ////////////////////////////////////////////////////////////////////
INSERT ##HVHPolicy
(
     PK_Policy                  
    ,PolicyReference            
    ,IsQuote                    
	,ExpiringPolicyReference	 
    ,IsAdmitted                 
    ,PlacingBrokerGroup         
    ,PlacingBrokerContact       
    ,PlacingBrokerState         
	,PlacingBrokerBranchName	 
    ,InsuredName                
    ,InsuredCity                
    ,InsuredState               
    ,InsuredCountry             
	,MarketSegment				 
	,UniqueMarketReference		 
    ,PolicyLimit        
	,PolicyDeductible        
	,LimitCurrency				 
    ,YearOfAccount              
    ,BrokerBeazleyTradeId       
)
 -- drop table #1
SELECT  
	 PK_Policy							= H.PK_Policy				 
	,PolicyReference					= PolicyReference
    ,IsQuote							= H.IsQuote
    ,ExpiringPolicyReference			= E.ExpiringPolicyReference  
    ,IsAdmitted							= H.IsAdmitted                 
    ,PlacingBrokerGroup					= '' -- H.PlacingBrokerGroup         
    ,PlacingBrokerContact				= H.PlacingBrokerContact       
    ,PlacingBrokerState					= H.PlacingBrokerState         
	,PlacingBrokerBranchName			= H.PlacingBrokerBranchName	 
    ,InsuredName						= H.InsuredName                
    ,InsuredCity						= H.InsuredCity                
    ,InsuredState						= H.InsuredState               
    ,InsuredCountry						= 'USA'          
	,MarketSegment						= 'MM'			 
	,UniqueMarketReference				= H.UniqueMarketReference		 
    ,PolicyLimit						= CONVERT(NUMERIC(19,4),H.PolicyLimit)
  	,PolicyDeductible       			= CASE WHEN ISNUMERIC(H.PolicyDeductible) = 0 THEN 0 ELSE  CONVERT(NUMERIC(19,4),H.PolicyDeductible)  END 
 	,PolLimitCurrency					= ISNULL(H.PolLimitCurrency, 'USD')		 
    ,YearOfAccount						= H.YearOfAccount              
    ,BrokerBeazleyTradeId				= H.BrokerBeazleyTradeId     
-- SELECT * 
FROM ##HVHUS H
INNER JOIN #LatestRecord L
		ON H.PK_Section = L.PK_Section
			AND L.SectionSequenceID = 1
INNER JOIN (
			SELECT PK_Policy, ExpiringPolicyReference = MAX(ExpiringPolicyReference)
			FROM ##HVHUS
			GROUP BY PK_Policy  
			) E
		ON H.PK_Policy = E.PK_Policy
WHERE H.PK_Policy IS NOT NULL
		AND IsQuote = 0
		AND DATEPART(YEAR,CONVERT(DATETIME,InceptionDate)) >= 2010 -- exclude YOA <2010
  
-- ////////////////////////////////////////////////////////////////////
-- SECTION INSERT 
-- ////////////////////////////////////////////////////////////////////
   
INSERT BeazleyIntelligenceExtractor.USHVH.HVHSection
( 
	 PK_Section							 
	,PolicyReference                     
    ,SectionReference                    
    ,SectionSequenceID                   
    ,CoverageName                        
    ,InceptionDate                       
    ,ExpiryDate                          
    ,SectionStatus                       
	,ExpiringSectionReference			 
	,FacilityReference                   
	,TriFocus                            
	,ClassOfBusinessCode                 
    ,BindDate                            
    ,ProductName                         
	,IsRenewal							 
	,Peril								 
	,LloydsRiskCode                      
    ,RiskLocation                        
    ,UnderwriterName                     
    ,OfficeLocation                      
    ,LeadInsurerPseudo                   
    ,TermsOfTradeDate                    
    ,IsSigned                            
    ,WrittenLine                         
    ,WrittenOrder                        
    ,SignedLine                          
    ,SignedOrder                         
    ,EstimatedSigning                    
    ,WrittenOrEstimatedPremium           
    ,SectionLimit                        
	,EventLimit							 
    ,LimitQualifier                      
    ,LimitCurrency                       
	,OriginalCurrency                    
    ,SettlementCurrency                  
	,ExcessAmount						 
	,ExcessQualifier					 
	,DeductibleAmount					 
	,DeductiblePercentage				 
	,DeductibleQualifier				 
    ,ExternalAcquisitionCostPercentage   
    ,ExternalAcquisitionCostAmount       
	,InternalAcquisitionCostPercentage	 
	,InternalAcquisitionCostAmount		 
	,RateChangePercentage				 
	,BenchmarkPremiumPercentage			 
	,BenchmarkPremiumAmount				 
	,OriginalToSettlementCurrencyRate	 
	,LimitToSettlementCurrencyRate		 
	,InsuredItemNarrative				 
	,Occupation
	,YearOfAccount							 
)

SELECT 
	 M.PK_Section
	,P.PolicyReference                     
    ,SectionReference							=   P.PolicyReference  + '-' + right('0'+cast(M.SectionSequenceID as varchar(2)),2) 
    ,M.SectionSequenceID                   
    ,M.CoverageName                        
    ,M.InceptionDate                       
    ,M.ExpiryDate                          
    ,SectionStatus								= CASE	WHEN M.SectionStatus = 'RENEWAL QUOTE'	THEN 'Quote'	-- IDEALLY, THIS SHOULD BE IN MDS
														WHEN M.SectionStatus = 'BOUND REINSTATE' THEN 'Live'
														WHEN M.SectionStatus = 'BOUND'			THEN 'Live'
														WHEN M.SectionStatus = 'BOUND RENEWAL'	THEN 'Live'
														WHEN M.SectionStatus = 'BOUND MTA'		THEN 'Live'
														WHEN M.SectionStatus = 'CANCELLED'		THEN 'Cancelled'
														WHEN M.SectionStatus = 'MTA QUOTE'		THEN 'Live'
														WHEN M.SectionStatus = 'QUOTED'			THEN 'Quote'
														WHEN M.SectionStatus = 'DOC VERSION FOR OUTLINE MTA'  THEN 'Live'
														WHEN M.SectionStatus = 'ERROR CORRECTION' THEN 'Cancelled'
														ELSE NULL
												  END
                  
	,ExpiringSectionReference		= P.ExpiringPolicyReference  + '-' + right('0'+cast(M.SectionSequenceID as varchar(2)),2) 	 
	,M.FacilityReference                   
	,M.TriFocus                            
	,M.ClassOfBusinessCode                 
    ,M.BindDate                            
    ,M.ProductName                         
	,IsRenewal				= CASE WHEN P.ExpiringPolicyReference IS NOT NULL THEN 1
								   WHEN P.ExpiringPolicyReference IS NULL     THEN 0
								   ELSE 0
								   END					 
	,M.Peril								 
	,M.LloydsRiskCode							
    ,RiskLocation			= CASE 	WHEN M.InsuredState = 'AL'	 THEN 'Alabama'
									WHEN M.InsuredState = 'AK'	 THEN 'Alaska'
									WHEN M.InsuredState = 'AZ'	 THEN 'Arizona'
									WHEN M.InsuredState = 'AR'	 THEN 'Arkansas'
									WHEN M.InsuredState = 'CA'	 THEN 'California'
									WHEN M.InsuredState = 'CO'	 THEN 'Colorado'
									WHEN M.InsuredState = 'CT'	 THEN 'Connecticut'
									WHEN M.InsuredState = 'DE'	 THEN 'Delaware'
									WHEN M.InsuredState = 'FL'	 THEN 'Florida'
									WHEN M.InsuredState = 'GA'	 THEN 'Georgia'
									WHEN M.InsuredState = 'HI'	 THEN 'Hawaii'
									WHEN M.InsuredState = 'ID'	 THEN 'Idaho'
									WHEN M.InsuredState = 'IL'	 THEN 'Illinois'
									WHEN M.InsuredState = 'IN'	 THEN 'Indiana'
									WHEN M.InsuredState = 'IA'	 THEN 'Iowa'
									WHEN M.InsuredState = 'KS'	 THEN 'Kansas'
									WHEN M.InsuredState = 'KY'	 THEN 'Kentucky'
									WHEN M.InsuredState = 'LA'	 THEN 'Louisiana'
									WHEN M.InsuredState = 'ME'	 THEN 'Maine'
									WHEN M.InsuredState = 'MD'	 THEN 'Maryland'
									WHEN M.InsuredState = 'MA'	 THEN 'Massachusetts'
									WHEN M.InsuredState = 'MI'	 THEN 'Michigan'
									WHEN M.InsuredState = 'MN'	 THEN 'Minnesota'
									WHEN M.InsuredState = 'MS'	 THEN 'Mississippi'
									WHEN M.InsuredState = 'MO'	 THEN 'Missouri'
									WHEN M.InsuredState = 'MT'	 THEN 'Montana'
									WHEN M.InsuredState = 'NE'	 THEN 'Nebraska'
									WHEN M.InsuredState = 'NV'	 THEN 'Nevada'
									WHEN M.InsuredState = 'NH'	 THEN 'New Hampshire'
									WHEN M.InsuredState = 'NJ'	 THEN 'New Jersey'
									WHEN M.InsuredState = 'NM'	 THEN 'New Mexico'
									WHEN M.InsuredState = 'NY'	 THEN 'New York'
									WHEN M.InsuredState = 'NC'	 THEN 'North Carolina'
									WHEN M.InsuredState = 'ND'	 THEN 'North Dakota'
									WHEN M.InsuredState = 'OH'	 THEN 'Ohio'
									WHEN M.InsuredState = 'OK'	 THEN 'Oklahoma'
									WHEN M.InsuredState = 'OR'	 THEN 'Oregon'
									WHEN M.InsuredState = 'PA'	 THEN 'Pennsylvania'
									WHEN M.InsuredState = 'RI'	 THEN 'Rhode Island'
									WHEN M.InsuredState = 'SC'	 THEN 'South Carolina'
									WHEN M.InsuredState = 'SD'	 THEN 'South Dakota'
									WHEN M.InsuredState = 'TN'	 THEN 'Tennessee'
									WHEN M.InsuredState = 'TX'	 THEN 'Texas'
									WHEN M.InsuredState = 'UT'	 THEN 'Utah'
									WHEN M.InsuredState = 'VT'	 THEN 'Vermont'
									WHEN M.InsuredState = 'VA'	 THEN 'Virginia'
									WHEN M.InsuredState = 'WA'	 THEN 'Washington'
									WHEN M.InsuredState = 'WV'	 THEN 'West Virginia'
									WHEN M.InsuredState = 'WI'	 THEN 'Wisconsin'
									WHEN M.InsuredState = 'WY'	 THEN 'Wyoming'
									ELSE M.InsuredState
								END
           
    ,UnderwriterName			= CASE 	WHEN M.UnderwriterName = '' THEN 'N/A'            
										ELSE M.UnderwriterName
								  END
    ,M.OfficeLocation								
    ,M.LeadInsurerPseudo                   
    ,M.TermsOfTradeDate                    
    ,M.IsSigned
    ,WrittenLine								= CONVERT(NUMERIC(19,4), M.WrittenLine)  
    ,WrittenOrder                        		= CONVERT(NUMERIC(19,12),M.WrittenOrder)                           
    ,SignedLine                          		= CONVERT(NUMERIC(19,12),M.SignedLine)                             
    ,SignedOrder                         		= CONVERT(NUMERIC(19,12),M.SignedOrder)                            
    ,EstimatedSigning                    		= CONVERT(NUMERIC(19,12),M.EstimatedSigning)                       
    ,WrittenOrEstimatedPremium           		= PREM.Premium
    ,SectionLimit                        		= CONVERT(NUMERIC(19,4) ,P_limit.PolicyLimit)                           
	,EventLimit							 		= CONVERT(NUMERIC(19,4) ,M.EventLimit)								
    ,M.LimitQualifier                      			 
--    ,M.SecLimitCurrency								 
	,P_limit.LimitCurrency
	,M.OriginalCurrency                    			 
    ,M.SettlementCurrency                  			 
	,ExcessAmount						 		= CONVERT(NUMERIC(19,4) ,M.ExcessAmount)							
	,ExcessQualifier					 		= CONVERT(NUMERIC(19,4) ,M.ExcessQualifier)						
	,DeductibleAmount					 		= CONVERT(NUMERIC(19,4) ,P_limit.PolicyDeductible)  						
	,DeductiblePercentage				 		= CONVERT(NUMERIC(19,4) ,M.DeductiblePercentage)					
	,DeductibleQualifier				 		= 'AOP'	  
    ,ExternalAcquisitionCostPercentage   		= CONVERT(NUMERIC(19,12),M.ExternalAcquisitionCostPercentage)      
    ,ExternalAcquisitionCostAmount       		= CONVERT(NUMERIC(19,4) ,M.ExternalAcquisitionCostPercentage)* CONVERT(NUMERIC(19,4) ,M.WrittenOrEstimatedPremium) /100
	,InternalAcquisitionCostPercentage	 		= CONVERT(NUMERIC(19,4) ,M.InternalAcquisitionCostPercentage)		
	,InternalAcquisitionCostAmount		 		= CONVERT(NUMERIC(19,4) ,M.InternalAcquisitionCostAmount)			
	,RateChangePercentage				 		= CONVERT(NUMERIC(19,4) ,M.RateChangePercentage)					
	,BenchmarkPremiumPercentage			 		= CONVERT(NUMERIC(19,4) ,M.BenchmarkPremiumPercentage	)			
	,BenchmarkPremiumAmount				 		= CONVERT(NUMERIC(19,4) ,M.BenchmarkPremiumAmount)					
	,OriginalToSettlementCurrencyRate	 		= CONVERT(NUMERIC(19,4) ,M.OriginalToSettlementCurrencyRate)		
	,LimitToSettlementCurrencyRate		 		= CONVERT(NUMERIC(19,4) ,M.LimitToSettlementCurrencyRate)			
	,M.InsuredItemNarrative				 		 
	,OccupancyDescription
	,YearOfAccount								= CONVERT(INT, P.YearOfAccount)

-- SELECT *   	 											 
FROM tempdb..##HVHUS M
INNER JOIN tempdb..##HVHPolicy P ON P.PK_policy = M.FK_policy
LEFT OUTER JOIN tempdb..##HVHPolicy P_limit ON P_limit.PK_policy = M.FK_policy AND M.SectionSequenceID = 1
INNER JOIN #LatestRecord L 
		ON M.PK_Section = L.PK_Section
INNER JOIN (SELECT FK_policy, SectionSequenceID, Premium = SUM(CONVERT(NUMERIC(19,4) ,WrittenOrEstimatedPremium))
			FROM tempdb..##HVHUS
			GROUP BY FK_policy, SectionSequenceID
			) PREM
		ON PREM.SectionSequenceID = M.SectionSequenceID AND PREM.FK_policy = M.FK_policy
WHERE 
	(M.PK_Section IS NOT NULL OR M.SectionReference IS NOT NULL)
	AND P.IsQuote = 0

  
-- ////////////////////////////////////////////////////////////////////
-- UPDATE BINDER REFS 
-- //////////////////////////////////////////////////////////////////// 
UPDATE BeazleyIntelligenceExtractor.USHVH.HVHSection
SET FacilityReference =  B.BinderReference
FROM BeazleyIntelligenceExtractor.USHVH.HVHSection S
LEFT OUTER JOIN ##HVHBinderMapping B
ON  S.YearOfAccount = B.YOA
WHERE (B.BinderReference LIKE '%B_YH' OR B.BinderReference LIKE '%B_3Y')  AND  S.ProductName = 'Excess Flood'


UPDATE BeazleyIntelligenceExtractor.USHVH.HVHSection
SET FacilityReference =  B.BinderReference
FROM BeazleyIntelligenceExtractor.USHVH.HVHSection S
LEFT OUTER JOIN  ##HVHBinderMapping  B
ON  S.YearOfAccount = B.YOA
WHERE (B.BinderReference LIKE '%C_YH' OR B.BinderReference LIKE '%C_3Y')  AND  S.ProductName = 'Excess Wind & Hail'

UPDATE BeazleyIntelligenceExtractor.USHVH.HVHSection
SET FacilityReference =  B.BinderReference
FROM BeazleyIntelligenceExtractor.USHVH.HVHSection S
LEFT OUTER JOIN ##HVHBinderMapping B
ON  S.YearOfAccount = B.YOA
WHERE((B.BinderReference  LIKE '%A_YH' OR B.BinderReference LIKE '%A_3Y')  AND  S.ProductName not in ('Excess Flood', 'Excess Wind & Hail') AND S.TriFocus <> 'BESI Homeowners')
OR ((B.BinderReference  LIKE '%A_YH' OR B.BinderReference LIKE '%A_3Y')  AND  S.ProductName = 'Excess Wind & Hail' AND s.YearOfAccount<2018);

-- BI-13024: BAU - US HVH Exposure, Limit and LimitCurrency are missing in Redcube/Datasets

UPDATE t1
SET t1.SectionLimit = t2.SectionLimit, t1.LimitCurrency = t2.LimitCurrency
FROM BeazleyIntelligenceExtractor.USHVH.HVHSection t1
INNER JOIN BeazleyIntelligenceExtractor.USHVH.HVHSection t2
ON t1.PolicyReference = t2.PolicyReference AND t2.SectionSequenceID = 1
WHERE t1.SectionLimit IS NULL;

-- ////////////////////////////////////////////////////////////////////
-- UPDATE MISSING RiskLocation 
-- //////////////////////////////////////////////////////////////////// 
WITH CTE 
AS 
(SELECT policyreference, RiskLocation = MAX(RiskLocation)
FROM BeazleyIntelligenceExtractor.USHVH.HVHSection
WHERE RiskLocation IS NOT NULL
GROUP BY policyreference
)
UPDATE A
SET RiskLocation = B.RiskLocation
FROM BeazleyIntelligenceExtractor.USHVH.HVHSection A
INNER JOIN CTE B
ON a.PolicyReference = b.PolicyReference
WHERE A.RiskLocation IS NULL 
 -- select RiskLocation,* from tempdb..##HVHSection where risklocation is null order by policyreference

-- ////////////////////////////////////////////////////////////////////
-- TRANSACTION INSERT 
-- ////////////////////////////////////////////////////////////////////
INSERT ##HVHTransaction
(
     FK_Section							
	,SectionReference					
	,AccountingPeriod					
	,DateTimeCreated					
	,TransactionClass					
	,ReceivedPremium					
	,ExternalAcquisitionCostPercentage	
	,ExternalAcquisitionCostAmount		
	,OriginalCurrency					
	,SettlementCurrency					
	,TaxPayable							
	,TaxLocation						
	,OriginalToSettlementCurrencyRate	
	,SourceSystemKey					
)

SELECT 
     FK_Section							= S.SectionReference
	,SectionReference					= S.SectionReference	
	,AccountingPeriod					= DATEADD(MONTH, DATEDIFF(MONTH,0,S.inceptiondate),0)			
	,DateTimeCreated					= CONVERT(datetime, S.inceptiondate)					
	,TransactionClass					= 'AAP'				
	,ReceivedPremium					= ISNULL(CONVERT(NUMERIC(19,4),S.writtenOrEstimatedPremium)	,0)			
	,ExternalAcquisitionCostPercentage	= ISNULL(CONVERT(NUMERIC(19,4),S.ExternalAcquisitionCostPercentage)	,0)
	,ExternalAcquisitionCostAmount		= --ISNULL(CONVERT(NUMERIC(19,4),M.ExternalAcquisitionCostAmount),0)		
										  ISNULL(CONVERT(NUMERIC(19,4),S.writtenOrEstimatedPremium)	,0) * ISNULL(CONVERT(NUMERIC(19,4),S.ExternalAcquisitionCostPercentage)	,0)
										  /100
	,OriginalCurrency					= S.OriginalCurrency					
	,SettlementCurrency					= S.SettlementCurrency					
	,TaxPayable							= 0					
	,TaxLocation						= NULL						
	,OriginalToSettlementCurrencyRate	= S.OriginalToSettlementCurrencyRate	
	,SourceSystemKey					= S.SectionReference
FROM    BeazleyIntelligenceExtractor.USHVH.HVHSection S