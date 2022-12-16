Do
$$
DECLARE v_rowCount int;
BEGIN
WITH medication_update AS
(
    WITH medication_codes AS
    (
        SELECT
            atc AS atc_code
            , fhir_logical_id AS logicalId
        FROM cds_etl_helper.medication_id_map
     )
     , medication_concepts AS
     (
        SELECT
            concept_id
            , concept_code
            , valid_start_date
            , valid_end_date
        FROM concept
        WHERE vocabulary_id = 'ATC'
     )
     , exist_drug AS
     (
        SELECT
            drug_exposure_id
            , drug_source_value
            , drug_exposure_start_date
        FROM drug_exposure
        WHERE drug_concept_id = 0
          AND drug_source_concept_id IS NULL
      )
    SELECT DISTINCT
        mc.logicalId
        , mcc.concept_id
        , mcc.concept_code
    FROM medication_codes mc
    LEFT JOIN medication_concepts mcc
           ON mc.atc_code = mcc.concept_code
    INNER JOIN exist_drug ed
           ON mc.logicalId = ed.drug_source_value
          AND ed.drug_exposure_start_date BETWEEN mcc.valid_start_date AND mcc.valid_end_date
)
UPDATE cds_cdm.drug_exposure de
SET    drug_source_value = COALESCE(mu.concept_code,de.drug_source_value)
       , drug_source_concept_id = COALESCE(mu.concept_id,0)
       , drug_concept_id = COALESCE(mu.concept_id,0)
FROM   medication_update mu
WHERE  de.drug_source_value = mu.logicalId;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Update % rows for drug_exposure.',v_rowCount;
END;
$$;
