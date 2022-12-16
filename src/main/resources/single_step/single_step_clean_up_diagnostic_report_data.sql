BEGIN;


DELETE
FROM observation
WHERE fhir_logical_id LIKE 'dir-%' OR fhir_identifier LIKE 'dir-%';

DELETE
FROM measurement
WHERE fhir_logical_id LIKE 'dir-%' OR fhir_identifier LIKE 'dir-%';

DELETE
FROM procedure_occurrence
WHERE fhir_logical_id LIKE 'dir-%' OR fhir_identifier LIKE 'dir-%';

COMMIT;
