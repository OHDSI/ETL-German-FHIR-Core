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
ALTER TABLE visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_preceding;

--Adds two new columns called fhir_logical_id and fhir_identifier to cds_etl_helper.post_process_map table
ALTER TABLE IF EXISTS cds_etl_helper.post_process_map ADD COLUMN IF NOT EXISTS fhir_logical_id varchar(250) NULL, ADD COLUMN IF NOT EXISTS fhir_identifier varchar(250) NULL;
--
----Alter drug_exposure table to automatically increment drug_exposure_id
--CREATE SEQUENCE IF NOT EXISTS drug_exposure_id_seq INCREMENT BY 1 START WITH 1;
--ALTER TABLE drug_exposure ALTER COLUMN drug_exposure_id SET DEFAULT nextval('drug_exposure_id_seq');
--
----Alter visit_detail table to automatically increment visit_detail_id
--CREATE SEQUENCE IF NOT EXISTS visit_detail_id_seq INCREMENT BY 1 START WITH 1;
--ALTER TABLE visit_detail ALTER COLUMN visit_detail_id SET DEFAULT nextval('visit_detail_id_seq');

--Alter device_exposure table to automatically increment device_exposure_id
--CREATE SEQUENCE IF NOT EXISTS device_exposure_id_seq INCREMENT BY 1 START WITH 1;
--ALTER TABLE device_exposure ALTER COLUMN device_exposure_id SET DEFAULT nextval('device_exposure_id_seq');

--Alter condition_era table to automatically increment condition_era_id
CREATE SEQUENCE IF NOT EXISTS condition_era_id_seq INCREMENT BY 1 START WITH 1;
ALTER TABLE condition_era ALTER COLUMN condition_era_id SET DEFAULT nextval('condition_era_id_seq');

--Alter drug_era table to automatically increment drug_era_id
CREATE SEQUENCE IF NOT EXISTS drug_era_id_seq INCREMENT BY 1 START WITH 1;
ALTER TABLE drug_era ALTER COLUMN drug_era_id SET DEFAULT nextval('drug_era_id_seq');

--Rename cds_etl_helper.data_persistant_map to cds_etl_helper.post_process_map
ALTER TABLE IF EXISTS cds_etl_helper.data_persistant_map RENAME TO post_process_map;

--Change foriegn keys for person_id to 'on delete cascade'
ALTER TABLE cds_cdm.procedure_occurrence DROP CONSTRAINT IF EXISTS fpk_procedure_person;
ALTER TABLE cds_cdm.procedure_occurrence ADD CONSTRAINT fpk_procedure_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.drug_exposure DROP CONSTRAINT IF EXISTS fpk_drug_person;
ALTER TABLE cds_cdm.drug_exposure ADD CONSTRAINT fpk_drug_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.observation DROP CONSTRAINT IF EXISTS fpk_observation_person;
ALTER TABLE cds_cdm.observation ADD CONSTRAINT fpk_observation_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.condition_occurrence DROP CONSTRAINT IF EXISTS fpk_condition_person;
ALTER TABLE cds_cdm.condition_occurrence ADD CONSTRAINT fpk_condition_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.visit_occurrence DROP CONSTRAINT IF EXISTS fpk_visit_person;
ALTER TABLE cds_cdm.visit_occurrence ADD CONSTRAINT fpk_visit_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.visit_detail DROP CONSTRAINT IF EXISTS fpk_v_detail_person;
ALTER TABLE cds_cdm.visit_detail ADD CONSTRAINT fpk_v_detail_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.observation_period DROP CONSTRAINT IF EXISTS fpk_observation_period_person;
ALTER TABLE cds_cdm.observation_period ADD CONSTRAINT fpk_observation_period_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.measurement DROP CONSTRAINT IF EXISTS fpk_measurement_person;
ALTER TABLE cds_cdm.measurement ADD CONSTRAINT fpk_measurement_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.death DROP CONSTRAINT IF EXISTS fpk_death_person;
ALTER TABLE cds_cdm.death ADD CONSTRAINT fpk_death_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.device_exposure DROP CONSTRAINT IF EXISTS fpk_device_person;
ALTER TABLE cds_cdm.device_exposure ADD CONSTRAINT fpk_device_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON DELETE CASCADE;

--Change foriegn keys for visit_occurrence_id to 'on delete cascade'
ALTER TABLE cds_cdm.procedure_occurrence DROP CONSTRAINT IF EXISTS fpk_procedure_visit;
ALTER TABLE cds_cdm.procedure_occurrence ADD CONSTRAINT fpk_procedure_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.drug_exposure DROP CONSTRAINT IF EXISTS fpk_drug_visit;
ALTER TABLE cds_cdm.drug_exposure ADD CONSTRAINT fpk_drug_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.observation DROP CONSTRAINT IF EXISTS fpk_observation_visit;
ALTER TABLE cds_cdm.observation ADD CONSTRAINT fpk_observation_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.condition_occurrence DROP CONSTRAINT IF EXISTS fpk_condition_visit;
ALTER TABLE cds_cdm.condition_occurrence ADD CONSTRAINT fpk_condition_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.visit_detail DROP CONSTRAINT IF EXISTS fpd_v_detail_visit;
ALTER TABLE cds_cdm.visit_detail ADD CONSTRAINT fpd_v_detail_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.measurement DROP CONSTRAINT IF EXISTS fpk_measurement_visit;
ALTER TABLE cds_cdm.measurement ADD CONSTRAINT fpk_measurement_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;

ALTER TABLE cds_cdm.device_exposure DROP CONSTRAINT IF EXISTS fpk_device_visit;
ALTER TABLE cds_cdm.device_exposure ADD CONSTRAINT fpk_device_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON DELETE CASCADE;


END
$$;
