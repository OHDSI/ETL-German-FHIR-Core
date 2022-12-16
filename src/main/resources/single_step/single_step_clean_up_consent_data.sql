BEGIN;


DELETE
FROM observation
WHERE fhir_logical_id LIKE 'cons-%' OR fhir_identifier LIKE 'cons-%';


COMMIT;
