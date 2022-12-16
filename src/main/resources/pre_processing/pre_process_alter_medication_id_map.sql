DO $$
BEGIN
IF EXISTS
(
    SELECT
        1
    FROM information_schema.columns
    WHERE table_name='medication_id_map'
      AND column_name='logical_id'
      AND table_schema='cds_etl_helper'
)
THEN
    DROP INDEX IF EXISTS cds_etl_helper.idx_fhir_identifier_logical_id_medication;
    ALTER TABLE cds_etl_helper.medication_id_map RENAME COLUMN logical_id TO fhir_logical_id;
END IF;
IF EXISTS
(
    SELECT
        1
    FROM information_schema.columns
    WHERE table_name='medication_id_map'
      AND column_name='identifier'
      AND table_schema='cds_etl_helper'
)
THEN
    DROP INDEX IF EXISTS cds_etl_helper.idx_fhir_logical_id_identifier_medication;
    ALTER TABLE cds_etl_helper.medication_id_map RENAME COLUMN identifier TO fhir_identifier;
END IF;
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_medication ON cds_etl_helper.medication_id_map (fhir_logical_id,fhir_identifier);
END
$$
