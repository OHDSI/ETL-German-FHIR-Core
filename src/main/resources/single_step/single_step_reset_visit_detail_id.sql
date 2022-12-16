BEGIN;

-- update visit_detail_id in measurement

CREATE TEMP TABLE temp_measurement AS
SELECT measurement_id,
       person_id,
       measurement_concept_id,
       measurement_date,
       measurement_datetime,
       measurement_time,
       measurement_type_concept_id,
       operator_concept_id,
       value_as_number,
       value_as_concept_id,
       unit_concept_id,
       range_low,
       range_high,
       provider_id,
       visit_occurrence_id,
       NULL::INT AS visit_detail_id,
       measurement_source_value,
       measurement_source_concept_id,
       unit_source_value,
       value_source_value
FROM cds_cdm.measurement;

TRUNCATE measurement;


INSERT INTO measurement
SELECT *
FROM temp_measurement;


DROP TABLE temp_measurement;

-- update visit_detail_id in observation

CREATE TEMP TABLE temp_observation AS
SELECT observation_id,
       person_id,
       observation_concept_id,
       observation_date,
       observation_datetime,
       observation_type_concept_id,
       value_as_number,
       value_as_string,
       value_as_concept_id,
       qualifier_concept_id,
       unit_concept_id,
       provider_id,
       visit_occurrence_id,
       NULL::INT AS visit_detail_id,
       observation_source_value,
       observation_source_concept_id,
       unit_source_value,
       qualifier_source_value
FROM cds_cdm.observation;

TRUNCATE observation;


INSERT INTO observation
SELECT *
FROM temp_observation;


DROP TABLE temp_observation;

-- update visit_detail_id in condition_occurrence

CREATE TEMP TABLE temp_condition_occurrence AS
SELECT condition_occurrence_id,
       person_id,
       condition_concept_id,
       condition_start_date,
       condition_start_datetime,
       condition_end_date,
       condition_end_datetime,
       condition_type_concept_id,
       stop_reason,
       provider_id,
       visit_occurrence_id,
       NULL::INT AS visit_detail_id,
       condition_source_value,
       condition_source_concept_id,
       condition_status_source_value,
       condition_status_concept_id,
       fhir_logical_id,
       fhir_identifier
FROM cds_cdm.condition_occurrence;

TRUNCATE condition_occurrence;


INSERT INTO condition_occurrence
SELECT *
FROM temp_condition_occurrence;


DROP TABLE temp_condition_occurrence;

-- update visit_detail_id in procedure_occurrence

CREATE TEMP TABLE temp_procedure_occurrence AS
SELECT procedure_occurrence_id,
       person_id,
       procedure_concept_id,
       procedure_date,
       procedure_datetime,
       procedure_type_concept_id,
       modifier_concept_id,
       quantity,
       provider_id,
       visit_occurrence_id,
       NULL::INT AS visit_detail_id,
       procedure_source_value,
       procedure_source_concept_id,
       modifier_source_value,
       fhir_logical_id,
       fhir_identifier
FROM cds_cdm.procedure_occurrence;

TRUNCATE procedure_occurrence;


INSERT INTO procedure_occurrence
SELECT *
FROM temp_procedure_occurrence;


DROP TABLE temp_procedure_occurrence;

-- update visit_detail_id in drug_exposure

CREATE TEMP TABLE temp_drug_exposure AS
SELECT drug_exposure_id,
       person_id,
       drug_concept_id,
       drug_exposure_start_date,
       drug_exposure_start_datetime,
       drug_exposure_end_date,
       drug_exposure_end_datetime,
       verbatim_end_date,
       drug_type_concept_id,
       stop_reason,
       refills,
       quantity,
       days_supply,
       sig,
       route_concept_id,
       lot_number,
       provider_id,
       visit_occurrence_id,
       NULL::INT AS visit_detail_id,
       drug_source_value,
       drug_source_concept_id,
       route_source_value,
       dose_unit_source_value,
       fhir_logical_id,
       fhir_identifier
FROM cds_cdm.drug_exposure;

TRUNCATE drug_exposure;


INSERT INTO drug_exposure
SELECT *
FROM temp_drug_exposure;


DROP TABLE temp_drug_exposure;

-- reset visit_detail table

ALTER TABLE visit_detail
DROP CONSTRAINT IF EXISTS fpk_v_detail_preceding;


DELETE
FROM visit_detail;


ALTER TABLE visit_detail ADD CONSTRAINT fpk_v_detail_preceding
FOREIGN KEY (preceding_visit_detail_id) REFERENCES visit_detail (visit_detail_id);


COMMIT;
