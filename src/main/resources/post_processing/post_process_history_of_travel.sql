--start date combines location
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH location_references AS
(
                SELECT DISTINCT substring(Split_part(data_one, ';', 2) for 50)                                                AS city,
                                substring(split_part(data_one, ';', 1) FOR 9)                                                 AS zip,
                                trim(substring(concat(split_part(data_one, ';', 1),' ',split_part(data_one, ';', 2)) FOR 50)) AS source_value,
                                substring(split_part(data_one, ';', 3) FOR 2)                                                 AS country,
                                substring(split_part(data_two,';',1) FOR 50)                                                  AS line1,
                                substring(split_part(data_two,';',1),51,101)                                                  AS line2,
                                substring(split_part(data_two,';',2) FOR 20)                                                  AS county,
                                fhir_logical_id ,
                                fhir_identifier
                FROM            cds_etl_helper.post_process_map ppm
                WHERE           ppm.omop_table = 'LOCATION'
                AND             ppm.type = 'OBSERVATION'), locations AS
(
                SELECT DISTINCT lr.fhir_logical_id,
                                lr.fhir_identifier,
                                l.location_id
                FROM            location_references lr
                JOIN            "location" l
                ON              l.address_1=lr.line1
                AND             l.address_2=lr.line2
                AND             l.city=lr.city
                AND             l.state=lr.country
                AND             l.zip=lr.zip
                AND             l.county=lr.county) ,travel_history_start AS
(
                SELECT DISTINCT o.observation_id,
                                lr.location_id,
                                o.fhir_logical_id,
                                o.fhir_identifier
                FROM            observation o
                JOIN            locations lr
                ON              o.fhir_logical_id=lr.fhir_logical_id
                WHERE           o.observation_concept_id =42528907 ) , primary_insert AS
(
            INSERT INTO cds_cdm.fact_relationship
                        (
                                    domain_concept_id_1,
                                    fact_id_1,
                                    domain_concept_id_2,
                                    fact_id_2,
                                    relationship_concept_id,
                                    fhir_logical_id_1,
                                    fhir_identifier_1,
                                    fhir_logical_id_2,
                                    fhir_identifier_2
                        )
            SELECT 27 ,
                   observation_id ,
                   0 ,
                   location_id ,
                   44818762 ,
                   fhir_logical_id,
                   fhir_identifier,
                   fhir_logical_id,
                   fhir_identifier
            FROM   travel_history_start returning * ), inserted_rows AS
(
            INSERT INTO cds_cdm.fact_relationship
                        (
                                    domain_concept_id_1,
                                    fact_id_1,
                                    domain_concept_id_2,
                                    fact_id_2,
                                    relationship_concept_id,
                                    fhir_logical_id_1,
                                    fhir_identifier_1,
                                    fhir_logical_id_2,
                                    fhir_identifier_2
                        )
            SELECT domain_concept_id_2,
                   fact_id_2 ,
                   domain_concept_id_1,
                   fact_id_1,
                   44818860,
                   fhir_logical_id_2,
                   fhir_identifier_2,
                   fhir_logical_id_1,
                   fhir_identifier_1
            FROM   primary_insert returning *)
UPDATE cds_etl_helper.post_process_map
SET    omop_id =1
FROM   inserted_rows ir
WHERE  omop_table ='LOCATION'
AND    "type" = 'OBSERVATION'
AND    fhir_logical_id = ir.fhir_logical_id_1;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for Start date and Location of Hitory-of-travel relationship.',v_rowCount*2;
END;
$$;

--end date combines location
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH location_references AS
(
                SELECT DISTINCT substring(Split_part(data_one, ';', 2) for 50)                                                AS city,
                                substring(split_part(data_one, ';', 1) FOR 9)                                                 AS zip,
                                trim(substring(concat(split_part(data_one, ';', 1),' ',split_part(data_one, ';', 2)) FOR 50)) AS source_value,
                                substring(split_part(data_one, ';', 3) FOR 2)                                                 AS country,
                                substring(split_part(data_two,';',1) FOR 50)                                                  AS line1,
                                substring(split_part(data_two,';',1),51,101)                                                  AS line2,
                                substring(split_part(data_two,';',2) FOR 20)                                                  AS county,
                                fhir_logical_id ,
                                fhir_identifier
                FROM            cds_etl_helper.post_process_map ppm
                WHERE           ppm.omop_table = 'LOCATION'
                AND             ppm.type = 'OBSERVATION'), locations AS
(
                SELECT DISTINCT lr.fhir_logical_id,
                                lr.fhir_identifier,
                                l.location_id
                FROM            location_references lr
                JOIN            "location" l
                ON              l.address_1=lr.line1
                AND             l.address_2=lr.line2
                AND             l.city=lr.city
                AND             l.state=lr.country
                AND             l.zip=lr.zip
                AND             l.county=lr.county) ,travel_history_start AS
(
                SELECT DISTINCT o.observation_id,
                                lr.location_id,
                                o.fhir_logical_id,
                                o.fhir_identifier
                FROM            observation o
                JOIN            locations lr
                ON              o.fhir_logical_id=lr.fhir_logical_id
                WHERE           o.observation_concept_id =37021024 ) , primary_insert AS
(
            INSERT INTO cds_cdm.fact_relationship
                        (
                                    domain_concept_id_1,
                                    fact_id_1,
                                    domain_concept_id_2,
                                    fact_id_2,
                                    relationship_concept_id,
                                    fhir_logical_id_1,
                                    fhir_identifier_1,
                                    fhir_logical_id_2,
                                    fhir_identifier_2
                        )
            SELECT 27,
                   observation_id,
                   0,
                   location_id,
                   44818762,
                   fhir_logical_id,
                   fhir_identifier,
                   fhir_logical_id,
                   fhir_identifier
            FROM   travel_history_start returning * ), inserted_rows AS
(
            INSERT INTO cds_cdm.fact_relationship
                        (
                                    domain_concept_id_1,
                                    fact_id_1,
                                    domain_concept_id_2,
                                    fact_id_2,
                                    relationship_concept_id,
                                    fhir_logical_id_1,
                                    fhir_identifier_1,
                                    fhir_logical_id_2,
                                    fhir_identifier_2
                        )
            SELECT domain_concept_id_2 ,
                   fact_id_2 ,
                   domain_concept_id_1 ,
                   fact_id_1 ,
                   44818860,
                   fhir_logical_id_2,
                   fhir_identifier_2,
                   fhir_logical_id_1,
                   fhir_identifier_1
            FROM   primary_insert returning *)
UPDATE cds_etl_helper.post_process_map
SET    omop_id =1
FROM   inserted_rows ir
WHERE  omop_table ='LOCATION'
AND    "type" = 'OBSERVATION'
AND    fhir_logical_id = ir.fhir_logical_id_1;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for End date and Location of Hitory-of-travel relationship.',v_rowCount*2;
END;
$$;

--start date to end date
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH start_date AS
(
       SELECT *
       FROM   observation o
       WHERE  o.observation_concept_id=42528907) , end_date AS
(
       SELECT *
       FROM   observation o2
       WHERE  o2.observation_concept_id=37021024) , start_end_pair AS
(
       SELECT sd.fhir_logical_id AS start_fhir_logical_id,
              sd.fhir_identifier AS start_fhir_identifier,
              sd.observation_id  AS start_observation_id,
              ed.fhir_logical_id AS end_fhir_logical_id,
              ed.fhir_identifier AS end_fhir_identifier,
              ed.observation_id  AS end_observation_id
       FROM   start_date sd
       JOIN   end_date ed
       ON     sd.fhir_logical_id = ed.fhir_logical_id) ,primary_insert AS
(
            insert INTO cds_cdm.fact_relationship
                        (
                                    domain_concept_id_1,
                                    fact_id_1,
                                    domain_concept_id_2,
                                    fact_id_2,
                                    relationship_concept_id,
                                    fhir_logical_id_1,
                                    fhir_identifier_1,
                                    fhir_logical_id_2,
                                    fhir_identifier_2
                        )
            SELECT 27,
                   start_observation_id ,
                   27,
                   end_observation_id ,
                   32714,
                   start_fhir_logical_id,
                   start_fhir_identifier,
                   end_fhir_logical_id,
                   end_fhir_identifier
            FROM   start_end_pair returning * )
INSERT INTO cds_cdm.fact_relationship
            (
                        domain_concept_id_1 ,
                        fact_id_1,
                        domain_concept_id_2 ,
                        fact_id_2,
                        relationship_concept_id,
                        fhir_logical_id_1,
                        fhir_identifier_1,
                        fhir_logical_id_2,
                        fhir_identifier_2
            )
SELECT domain_concept_id_2,
       fact_id_2,
       domain_concept_id_1,
       fact_id_1,
       32712,
       fhir_logical_id_2,
       fhir_identifier_2,
       fhir_logical_id_1,
       fhir_identifier_1
FROM   primary_insert;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for Start and End date of History-of-Travel relationship.',v_rowCount*2;
END;
$$;
