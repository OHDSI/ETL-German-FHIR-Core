BEGIN;


DELETE
FROM drug_exposure
WHERE fhir_logical_id LIKE 'imm-%' OR fhir_identifier LIKE 'imm-%';


COMMIT;
