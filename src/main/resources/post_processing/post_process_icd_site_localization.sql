-- ICD site localization
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH icdsitelocalization AS
(
    SELECT
        split_part(data_one, ':', 1) AS site_localization
        , split_part(data_one, ':', 2) AS site_localization_domain
        , split_part(data_two, ':', 1) AS icd_code
        , split_part(data_two, ':', 2) AS icd_domain
        , fhir_logical_id
        , fhir_identifier
    FROM cds_etl_helper.post_process_map
    WHERE type = 'CONDITION' AND omop_table = 'site_localization'
)
, fact_relationship_map AS
(
    SELECT DISTINCT
        sl.icd_code
        , sl.icd_domain
        , d.domain_concept_id AS icd_domain_code
        , coalesce(p.procedure_occurrence_id, c.condition_occurrence_id, m.measurement_id, o.observation_id) AS icd_id
        , sl.site_localization
        , sl.site_localization_domain
        , dd.domain_concept_id AS site_domain_code
        , oo.observation_id AS site_localization_id
       , sl.fhir_logical_id,sl.fhir_identifier
    FROM icdsitelocalization sl
    LEFT JOIN procedure_occurrence p
           ON sl.icd_code = p.procedure_source_value
          AND sl.fhir_logical_id = p.fhir_logical_id
          --AND sl.fhir_identifier = p.fhir_identifier
    LEFT JOIN condition_occurrence c
           ON sl.icd_code = c.condition_source_value
          AND sl.fhir_logical_id = c.fhir_logical_id
          --AND sl.fhir_identifier = c.fhir_identifier
    LEFT JOIN measurement m
           ON sl.icd_code = m.measurement_source_value
          AND sl.fhir_logical_id = m.fhir_logical_id
          --AND sl.fhir_identifier = m.fhir_identifier
    LEFT JOIN observation o
           ON sl.icd_code = o.observation_source_value
          AND sl.fhir_logical_id = o.fhir_logical_id
          --AND sl.fhir_identifier = o.fhir_identifier
    LEFT JOIN observation oo
           ON sl.site_localization = oo.observation_source_value
          AND sl.fhir_logical_id = oo.fhir_logical_id
         -- AND sl.fhir_identifier = oo.fhir_identifier
    LEFT JOIN domain d
           ON sl.icd_domain = d.domain_id
    LEFT JOIN domain dd
           ON sl.site_localization_domain = dd.domain_id
)
, primary_insert AS
(
    INSERT INTO cds_cdm.fact_relationship
    (
        domain_concept_id_1
        , fact_id_1
        , domain_concept_id_2
        , fact_id_2
        , relationship_concept_id,
		fhir_logical_id_1,
		fhir_identifier_1,
		fhir_logical_id_2,
		fhir_identifier_2
    )
    SELECT
        icd_domain_code
        , icd_id
        , site_domain_code
        , site_localization_id
        , 44818762,fhir_logical_id,fhir_identifier,fhir_logical_id,fhir_identifier
    FROM fact_relationship_map
    WHERE icd_id IS NOT NULL
      AND site_localization_id IS NOT NULL
    RETURNING *
)
INSERT INTO cds_cdm.fact_relationship
(
	domain_concept_id_1
	, fact_id_1
	, domain_concept_id_2
	, fact_id_2
	, relationship_concept_id,
		fhir_logical_id_1,
		fhir_identifier_1,
		fhir_logical_id_2,
		fhir_identifier_2
)
SELECT
    domain_concept_id_2
    , fact_id_2
    , domain_concept_id_1
    , fact_id_1
    , 44818860,fhir_logical_id_2,fhir_identifier_2,fhir_logical_id_1,fhir_identifier_1
FROM primary_insert;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for ICD site localization.',v_rowCount*2;
END;
$$;
