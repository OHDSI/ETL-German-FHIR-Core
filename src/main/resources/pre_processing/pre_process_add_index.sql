Do
$$
BEGIN
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_person ON person (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_visit_occurrence ON visit_occurrence (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_visit_detail ON visit_detail (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_observation ON observation (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_measurement ON measurement (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_procedure_occurrence ON procedure_occurrence (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_death ON death (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_drug_exposure ON drug_exposure (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_condition_occurrence ON condition_occurrence (fhir_logical_id ASC, fhir_identifier ASC);
CREATE INDEX IF NOT EXISTS idx_fhir_logical_id_identifier_post_process ON cds_etl_helper.post_process_map (fhir_logical_id,fhir_identifier);
CREATE INDEX IF NOT EXISTS idx_data_one ON cds_etl_helper.post_process_map (data_one ASC);
CREATE INDEX IF NOT EXISTS idx_data_two ON cds_etl_helper.post_process_map (data_two ASC);
END;
$$;
