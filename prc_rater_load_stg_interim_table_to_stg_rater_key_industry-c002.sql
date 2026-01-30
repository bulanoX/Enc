 CREATE OR REPLACE PROCEDURE PXMGT_RATING_000_COD.PRC_RATER_LOAD_STG_INTERIM_TABLE_TO_STG_RATER_KEY_INDUSTRY()
 RETURNS VARCHAR(16777216)
 LANGUAGE SQL
 EXECUTE AS OWNER
 AS 'BEGIN
 /***************************************************************************************************
 Procedure:          PXMGT_RATING_000_COD.PRC_RATER_LOAD_STG_INTERIM_TABLE_TO_STG_RATER_KEY_INDUSTRY()
 Create Date:        10 May 2024
 Author:             Andreea-Elena Radu
 Description:        Fetch data from interim table to RATER_KEY_INDUSTRY table
 Call by:            A task with requisite role and permissions provided.
 Usage:              CALL PXMGT_RATING_000_COD.PRC_RATER_LOAD_STG_INTERIM_TABLE_TO_STG_RATER_KEY_INDUSTRY();
 ****************************************************************************************************
 SUMMARY OF CHANGES
 Date(dd Mmm yyyy)   Author                  Comments
 ------------------- ------------------- ------------------------------------------------------------
 30 July 2025        Andreea Macelaru        v1.2 - rename column from BK_KEY_INDUSTRY to KEY_INDUSTRY_KEY
 15 November 2024    Andreea Macelaru        v1.1 - change source column for BK_KEY_INDUSTRY
 10 May 2024         Andreea-Elena Radu      v1.0 - Initial script
 ***************************************************************************************************/

     INSERT INTO PXMGT_RATING_020_STG.RATER_KEY_INDUSTRY 
                 (
        
       "Key_Industry": { 
      "Code": null,
      "Code_Name": null, CODE_NAME
      "Code_Str": "423430", KEY_INDUSTRY_KEY
      "Code_Type": "NAICS", CODE_TYPE
      "Rater_Defined": null
                
     SELECT          t.BK_RATER_NAME                           
     	           ,t.BK_VERSION
     	           ,t.BK_RATING_ID
     	           ,t.KEY_INDUSTRY_KEY
     	           ,t.BK_CLIENT_SUPPLIED_ID
     	           ,t.CODE_NAME
     	           ,t.CODE_TYPE
     	           ,t.EVENT_LOAD_TIMESTAMP
     	           ,t.RECORD_KAFKA_NPTS
     	           ,t.RECORD_KAFKA_OFFSET
     	           ,t.RECORD_KAFKA_PARTITION
     	           ,t.CREATED_AT 
     	           ,t.CREATED_BY
     	           ,t.CREATED_FROM
     	           ,t.PROCESS_ID
     	           ,t.SOURCE_NAME  
                   
     FROM       (
                     SELECT       i.BK_RATER_NAME                                            AS BK_RATER_NAME
                                 ,i.BK_VERSION                                               AS BK_VERSION
                                 ,i.BK_RATING_ID                                             AS BK_RATING_ID
                                 ,RECORD_CONTENT:Result:Key_Industry:Code_Str::STRING        AS KEY_INDUSTRY_KEY
                                 ,i.BK_CLIENT_SUPPLIED_ID                                    AS BK_CLIENT_SUPPLIED_ID     
                                 ,RECORD_CONTENT:Result:Key_Industry:Code_Name::STRING       AS CODE_NAME
                                 ,RECORD_CONTENT:Result:Key_Industry:Code_Type::STRING       AS CODE_TYPE
                                 ,i.EVENT_LOAD_TIMESTAMP                                     AS EVENT_LOAD_TIMESTAMP                 
                                 ,i.RECORD_KAFKA_NPTS                                        AS RECORD_KAFKA_NPTS                               
                                 ,i.RECORD_KAFKA_OFFSET                                      AS RECORD_KAFKA_OFFSET   
                                 ,i.RECORD_KAFKA_PARTITION                                   AS RECORD_KAFKA_PARTITION
                                 ,CURRENT_TIMESTAMP::TIMESTAMP_NTZ                           AS CREATED_AT                           
                                 ,i.CREATED_BY                                               AS CREATED_BY         
                                 ,i.CREATED_FROM                                             AS CREATED_FROM
                                 ,i.PROCESS_ID                                               AS PROCESS_ID
                                 ,i.SOURCE_NAME                                              AS SOURCE_NAME  
                                
                     FROM        PXMGT_RATING_020_STG.RATER_GRS_INTERIM i 
                     LEFT JOIN   PXMGT_RATING_020_STG.RATER_KEY_INDUSTRY ki
                    
                             ON  i.BK_RATER_NAME         = ki.BK_RATER_NAME
                             AND i.BK_VERSION            = ki.BK_VERSION
                             AND i.BK_RATING_ID          = ki.BK_RATING_ID
                             AND i.BK_CLIENT_SUPPLIED_ID = ki.BK_CLIENT_SUPPLIED_ID
                            
                     WHERE       ki.BK_RATING_ID IS NULL 
                 ) t
                
     WHERE       t.KEY_INDUSTRY_KEY IS NOT NULL;
             --AND (t.CODE_NAME IS NOT NULL OR t.CODE_TYPE IS NOT NULL)

	
 	RETURN (''Number of rows inserted: '' || (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))));
 
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
 END';