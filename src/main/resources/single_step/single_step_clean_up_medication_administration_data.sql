BEGIN;


DELETE
FROM drug_exposure
WHERE fhir_logical_id LIKE 'mea-%' OR fhir_identifier LIKE 'mea-%';


COMMIT;
