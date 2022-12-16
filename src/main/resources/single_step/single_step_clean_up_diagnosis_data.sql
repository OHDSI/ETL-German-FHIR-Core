BEGIN;


DELETE
FROM fact_relationship
WHERE fhir_logical_id_1 LIKE 'con-%' OR fhir_identifier_1 LIKE 'con-%' OR fhir_logical_id_2 LIKE 'con-%' OR fhir_identifier_2 LIKE 'con-%';


DELETE
FROM cds_etl_helper.post_process_map
WHERE TYPE='CONDITION';


DELETE
FROM condition_occurrence
WHERE fhir_logical_id LIKE 'con-%' OR fhir_identifier LIKE 'con-%';


DELETE
FROM measurement
WHERE fhir_logical_id LIKE 'con-%' OR fhir_identifier LIKE 'con-%';


DELETE
FROM procedure_occurrence
WHERE fhir_logical_id LIKE 'con-%' OR fhir_identifier LIKE 'con-%';


DELETE
FROM observation
WHERE fhir_logical_id LIKE 'con-%' OR fhir_identifier LIKE 'con-%';


-- update written-flag in post_process_map for rank and use
UPDATE
cds_etl_helper.post_process_map
SET omop_id = 0
WHERE omop_table = 'use' or omop_table = 'rank';


COMMIT;
