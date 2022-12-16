-- Diagnosis stage
DO
$$
DECLARE v_rowCount INT;
BEGIN
WITH
    stage AS
    (   SELECT
            split_part(data_one, ':', 1) AS stage,
            split_part(data_one, ':', 2) AS stage_domain,
            split_part(data_two, ':', 1) AS diagnosis_code,
            split_part(data_two, ':', 2) AS diagnosis_domain,
            fhir_logical_id,
            fhir_identifier,
            4106767 AS qualifier_concept_id
        FROM
            cds_etl_helper.post_process_map
        WHERE
            TYPE = 'CONDITION'
        AND omop_table = 'stage'
    )
    ,
    fact_relationship_map AS
    (   SELECT
            DISTINCT s.diagnosis_code,
            s.diagnosis_domain,
            d.domain_concept_id AS diagnosis_domain_code,
            COALESCE(p.procedure_occurrence_id, c.condition_occurrence_id, m.measurement_id,
            o.observation_id) AS diagnosis_id,
            s.stage,
            s.stage_domain,
            dd.domain_concept_id AS stage_domain_code,
            oo.observation_id    AS stage_id,
            s.fhir_logical_id,
            s.fhir_identifier
        FROM
            stage s
        LEFT JOIN
            procedure_occurrence p
        ON
            s.diagnosis_code = p.procedure_source_value
        AND s.fhir_logical_id = p.fhir_logical_id
        LEFT JOIN
            condition_occurrence c
        ON
            s.diagnosis_code = c.condition_source_value
        AND s.fhir_logical_id = c.fhir_logical_id
        LEFT JOIN
            measurement m
        ON
            s.diagnosis_code = m.measurement_source_value
        AND s.fhir_logical_id = m.fhir_logical_id
        LEFT JOIN
            observation o
        ON
            s.diagnosis_code = o.observation_source_value
        AND s.fhir_logical_id = o.fhir_logical_id
        LEFT JOIN
            observation oo
        ON
            s.stage = oo.observation_source_value
        AND s.fhir_logical_id = oo.fhir_logical_id
        AND s.qualifier_concept_id = oo.qualifier_concept_id
        LEFT JOIN
            domain d
        ON
            s.diagnosis_domain = d.domain_id
        LEFT JOIN
            domain dd
        ON
            s.stage_domain = dd.domain_id
    )
    ,
    primary_insert AS
    ( INSERT INTO
            cds_cdm.fact_relationship
            (
                domain_concept_id_1,
                fact_id_1,
                domain_concept_id_2,
                fact_id_2,
                relationship_concept_id,
                fhir_logical_id_1,
                fhir_identifier_1,
                fhir_logical_id_2,
                fhir_identifier_2
            )
        SELECT
            diagnosis_domain_code,
            diagnosis_id,
            stage_domain_code,
            stage_id,
            45754814,
            fhir_logical_id,
            fhir_identifier,
            fhir_logical_id,
            fhir_identifier
        FROM
            fact_relationship_map
        WHERE
            diagnosis_id IS NOT NULL
        AND stage_id IS NOT NULL RETURNING *
    )
INSERT INTO
    cds_cdm.fact_relationship
    (
        domain_concept_id_1,
        fact_id_1,
        domain_concept_id_2,
        fact_id_2,
        relationship_concept_id,
        fhir_logical_id_1,
        fhir_identifier_1,
        fhir_logical_id_2,
        fhir_identifier_2
    )
SELECT
    domain_concept_id_2,
    fact_id_2,
    domain_concept_id_1,
    fact_id_1,
    45754815,
    fhir_logical_id_2,
    fhir_identifier_2,
    fhir_logical_id_1,
    fhir_identifier_1
FROM
    primary_insert;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for stage.',v_rowCount*2;
END;
$$;
