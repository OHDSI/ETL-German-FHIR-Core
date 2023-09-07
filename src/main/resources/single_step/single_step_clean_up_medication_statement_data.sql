BEGIN;


DELETE
FROM drug_exposure
WHERE fhir_logical_id LIKE 'mes-%' OR fhir_identifier LIKE 'mes-%';

DELETE
FROM observation
WHERE fhir_logical_id LIKE 'mes-%' OR fhir_identifier LIKE 'mes-%';


COMMIT;
