DO
$$
DECLARE v_rowCount int;
BEGIN
WITH dev_pro AS
(
       SELECT de.device_exposure_id ,
              po.procedure_occurrence_id ,
              po.fhir_logical_id AS pro_fhir_logical_id ,
              po.fhir_identifier AS pro_fhir_identifier,
              de.fhir_logical_id AS dev_fhir_logical_id,
              de.fhir_identifier AS dev_fhir_identifier,
              17                 AS device_domain,
              10                 AS procedure_domain
       FROM   device_exposure de
       JOIN   procedure_occurrence po
       ON     de.fhir_logical_id = po.fhir_logical_id ) ,primary_insert AS
(
            insert INTO cds_cdm.fact_relationship
                        (
                                    domain_concept_id_1 ,
                                    fact_id_1 ,
                                    domain_concept_id_2 ,
                                    fact_id_2 ,
                                    relationship_concept_id,
                                    fhir_logical_id_1,
                                    fhir_identifier_1,
                                    fhir_logical_id_2,
                                    fhir_identifier_2
                        )
            SELECT device_domain,
                   device_exposure_id,
                   procedure_domain,
                   procedure_occurrence_id,
                   44818892,
                   dev_fhir_logical_id,
                   dev_fhir_identifier,
                   pro_fhir_logical_id,
                   pro_fhir_identifier
            FROM   dev_pro dp
            WHERE  device_exposure_id IS NOT NULL
            AND    procedure_occurrence_id IS NOT NULL returning *)
INSERT INTO cds_cdm.fact_relationship
            (
                        domain_concept_id_1 ,
                        fact_id_1 ,
                        domain_concept_id_2 ,
                        fact_id_2 ,
                        relationship_concept_id,
                        fhir_logical_id_1,
                        fhir_identifier_1,
                        fhir_logical_id_2,
                        fhir_identifier_2
            )
SELECT domain_concept_id_2 ,
       fact_id_2,
       domain_concept_id_1 ,
       fact_id_1,
       44818794,
       fhir_logical_id_2,
       fhir_identifier_2,
       fhir_logical_id_1,
       fhir_identifier_1
FROM   primary_insert;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for Device and Procedure relationship.',v_rowCount*2;
END;
$$;
