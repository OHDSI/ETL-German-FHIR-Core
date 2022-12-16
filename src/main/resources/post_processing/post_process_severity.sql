-- Diagnosis severity
DO
$$
DECLARE v_rowCount INT;
BEGIN
WITH
    severity AS
    (   SELECT
            split_part(data_one, ':', 1) AS severity,
            split_part(data_one, ':', 2) AS severity_domain,
            split_part(data_two, ':', 1) AS icd_code,
            split_part(data_two, ':', 2) AS icd_domain,
            fhir_logical_id,
            fhir_identifier,
            4077563 AS qualifier_concept_id
        FROM
            cds_etl_helper.post_process_map
        WHERE
            TYPE = 'CONDITION'
        AND omop_table = 'severity'
    )
    ,
    fact_relationship_map AS
    (   SELECT
            DISTINCT s.icd_code,
            s.icd_domain,
            d.domain_concept_id AS icd_domain_code,
            COALESCE(p.procedure_occurrence_id, c.condition_occurrence_id, m.measurement_id,
            o.observation_id) AS icd_id,
            s.severity,
            s.severity_domain,
            dd.domain_concept_id AS severity_domain_code,
            oo.observation_id    AS severity_id,
            s.fhir_logical_id,
            s.fhir_identifier
        FROM
            severity s
        LEFT JOIN
            procedure_occurrence p
        ON
            s.icd_code = p.procedure_source_value
        AND s.fhir_logical_id = p.fhir_logical_id
        LEFT JOIN
            condition_occurrence c
        ON
            s.icd_code = c.condition_source_value
        AND s.fhir_logical_id = c.fhir_logical_id
        LEFT JOIN
            measurement m
        ON
            s.icd_code = m.measurement_source_value
        AND s.fhir_logical_id = m.fhir_logical_id
        LEFT JOIN
            observation o
        ON
            s.icd_code = o.observation_source_value
        AND s.fhir_logical_id = o.fhir_logical_id
        LEFT JOIN
            observation oo
        ON
            s.severity = oo.observation_source_value
        AND s.fhir_logical_id = oo.fhir_logical_id
        AND s.qualifier_concept_id = oo.qualifier_concept_id
        LEFT JOIN
            domain d
        ON
            s.icd_domain = d.domain_id
        LEFT JOIN
            domain dd
        ON
            s.severity_domain = dd.domain_id
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
            icd_domain_code,
            icd_id,
            severity_domain_code,
            severity_id,
            44818747,
            fhir_logical_id,
            fhir_identifier,
            fhir_logical_id,
            fhir_identifier
        FROM
            fact_relationship_map
        WHERE
            icd_id IS NOT NULL
        AND severity_id IS NOT NULL RETURNING *
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
    44818845,
    fhir_logical_id_2,
    fhir_identifier_2,
    fhir_logical_id_1,
    fhir_identifier_1
FROM
    primary_insert;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for severity.',v_rowCount*2;
END;
$$;
