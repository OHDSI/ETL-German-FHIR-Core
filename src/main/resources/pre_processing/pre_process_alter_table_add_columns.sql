Do $$
BEGIN
--Adds two new columns called fhir_logical_id and fhir_identifier to tables in OMOP CDM.
ALTER TABLE person ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE visit_occurrence ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE visit_detail ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE observation ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE measurement ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE procedure_occurrence ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE death ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE drug_exposure ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE condition_occurrence ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
ALTER TABLE device_exposure ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;

ALTER TABLE fact_relationship ADD COLUMN IF NOT EXISTS fhir_logical_id_1 varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier_1 varchar(250) NULL;
ALTER TABLE fact_relationship ADD COLUMN IF NOT EXISTS fhir_logical_id_2 varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier_2 varchar(250) NULL;

--Temporarily deleted the FK constraints for the column precending_visit_detail_id
--ALTER TABLE visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_preceding;
--COMMIT;
--ALTER TABLE visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_parent;
--COMMIT;

--Adds two new columns called fhir_logical_id and fhir_identifier to cds_etl_helper.post_process_map table
ALTER TABLE IF EXISTS cds_etl_helper.post_process_map ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;

--Alter drug_exposure table to automatically increment drug_exposure_id
--CREATE SEQUENCE IF NOT EXISTS drug_exposure_id_seq INCREMENT BY 1 START WITH 1;
--ALTER TABLE drug_exposure ALTER COLUMN drug_exposure_id SET DEFAULT nextval('drug_exposure_id_seq');

--Alter visit_detail table to automatically increment visit_detail_id
--CREATE SEQUENCE IF NOT EXISTS visit_detail_id_seq INCREMENT BY 1 START WITH 1;
--ALTER TABLE visit_detail ALTER COLUMN visit_detail_id SET DEFAULT nextval('visit_detail_id_seq');

--Alter device_exposure table to automatically increment device_exposure_id
--CREATE SEQUENCE IF NOT EXISTS device_exposure_id_seq INCREMENT BY 1 START WITH 1;
--ALTER TABLE device_exposure ALTER COLUMN device_exposure_id SET DEFAULT nextval('device_exposure_id_seq');




END
$$;

--Rename cds_etl_helper.data_persistant_map to cds_etl_helper.post_process_map
DO
$$
BEGIN
IF NOT EXISTS (select * from pg_catalog.pg_tables where schemaname='cds_etl_helper' and tablename='post_process_map')
THEN
ALTER TABLE IF EXISTS cds_etl_helper.data_persistant_map RENAME TO post_process_map;
END IF;
END
$$;
