-- primary ICD and secondary ICD
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH icdpair AS
(
    SELECT
        split_part(data_one, ':', 1)   AS primary_code
        , split_part(data_one, ':', 2) AS primary_domain
        , split_part(data_two, ':', 1) AS secondary_code
        , split_part(data_two, ':', 2) AS secondary_domain
        , fhir_logical_id
        , fhir_identifier
    FROM cds_etl_helper.post_process_map
    WHERE omop_table = 'primary_secondary_icd' AND omop_id = 0
)
, fact_relationship_map AS
(
    SELECT DISTINCT
        primary_code
        , primary_domain
        , d.domain_concept_id AS primary_domain_code
        , COALESCE(p.procedure_occurrence_id, c.condition_occurrence_id, m.measurement_id, o.observation_id) AS primary_id
        , secondary_code
        , secondary_domain
        , dd.domain_concept_id AS secondary_domain_code
        , COALESCE(pp.procedure_occurrence_id, cc.condition_occurrence_id, mm.measurement_id, oo.observation_id) AS secondary_id
        , ip.fhir_logical_id
        , ip.fhir_identifier
    FROM icdpair ip
    LEFT JOIN cds_cdm.procedure_occurrence p
           ON ip.primary_code = p.procedure_source_value
          AND ip.fhir_logical_id = p.fhir_logical_id
          --AND ip.fhir_identifier = p.fhir_identifier
    LEFT JOIN cds_cdm.procedure_occurrence pp
           ON ip.secondary_code = pp.procedure_source_value
          AND ip.fhir_logical_id = pp.fhir_logical_id
          --AND ip.fhir_identifier = pp.fhir_identifier
    LEFT JOIN cds_cdm.condition_occurrence c
           ON ip.primary_code = c.condition_source_value
          AND ip.fhir_logical_id = c.fhir_logical_id
          --AND ip.fhir_identifier = c.fhir_identifier
    LEFT JOIN cds_cdm.condition_occurrence cc
           ON ip.secondary_code = cc.condition_source_value
          AND ip.fhir_logical_id = cc.fhir_logical_id
          --AND ip.fhir_identifier = cc.fhir_identifier
    LEFT JOIN cds_cdm.measurement m
           ON ip.primary_code = m.measurement_source_value
          AND ip.fhir_logical_id = m.fhir_logical_id
          --AND ip.fhir_identifier = m.fhir_identifier
    LEFT JOIN cds_cdm.measurement mm
           ON ip.secondary_code = mm.measurement_source_value
          AND ip.fhir_logical_id = mm.fhir_logical_id
          --AND ip.fhir_identifier = mm.fhir_identifier
    LEFT JOIN cds_cdm.observation o
           ON ip.primary_code = o.observation_source_value
          AND ip.fhir_logical_id = o.fhir_logical_id
          --AND ip.fhir_identifier = o.fhir_identifier
    LEFT JOIN cds_cdm.observation oo
           ON ip.secondary_code = oo.observation_source_value
          AND ip.fhir_logical_id = oo.fhir_logical_id
          --AND ip.fhir_identifier = oo.fhir_identifier
    LEFT JOIN cds_cdm.domain d
           ON ip.primary_domain = d.domain_id
    LEFT JOIN cds_cdm.domain dd
           ON ip.secondary_domain = dd.domain_id
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
        primary_domain_code
        , primary_id
        , secondary_domain_code
        , secondary_id
        , 44818770,fhir_logical_id,fhir_identifier,fhir_logical_id,fhir_identifier
    FROM fact_relationship_map
    WHERE primary_id IS NOT NULL
      AND secondary_id IS NOT NULL
    RETURNING *
), inserted_rows AS(
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
    , 44818868,fhir_logical_id_2,fhir_identifier_2,fhir_logical_id_1,fhir_identifier_1
FROM primary_insert returning *)

UPDATE cds_etl_helper.post_process_map
  SET    omop_id =1
  FROM   inserted_rows ir
  WHERE  omop_table ='primary_secondary_icd'
  AND    fhir_logical_id = ir.fhir_logical_id_1;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for primary and secondary ICD.',v_rowCount*2;
END;
$$;
