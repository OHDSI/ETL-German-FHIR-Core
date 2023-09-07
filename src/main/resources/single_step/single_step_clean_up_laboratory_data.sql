BEGIN;


DELETE
FROM observation
WHERE fhir_logical_id LIKE 'obs-%' OR fhir_identifier LIKE 'obs-%';

DELETE
FROM measurement
WHERE fhir_logical_id LIKE 'obs-%' OR fhir_identifier LIKE 'obs-%';

DELETE
FROM procedure_occurrence
WHERE fhir_logical_id LIKE 'obs-%' OR fhir_identifier LIKE 'obs-%';


COMMIT;
