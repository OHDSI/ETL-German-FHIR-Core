BEGIN;


DELETE
FROM procedure_occurrence
WHERE fhir_logical_id LIKE 'pro-%' OR fhir_identifier LIKE 'pro-%';

DELETE
FROM device_exposure
WHERE fhir_logical_id LIKE 'pro-%' OR fhir_identifier LIKE 'pro-%';

DELETE
FROM observation
WHERE fhir_logical_id LIKE 'pro-%' OR fhir_identifier LIKE 'pro-%';

DELETE
FROM measurement
WHERE fhir_logical_id LIKE 'pro-%' OR fhir_identifier LIKE 'pro-%';

DELETE
FROM drug_exposure
WHERE fhir_logical_id LIKE 'pro-%' OR fhir_identifier LIKE 'pro-%';

DELETE
FROM fact_relationship
WHERE fhir_logical_id_1 LIKE 'pro-%' OR fhir_identifier_1 LIKE 'pro-%' OR fhir_logical_id_2 LIKE 'pro-%' OR fhir_identifier_2 LIKE 'pro-%';

COMMIT;
