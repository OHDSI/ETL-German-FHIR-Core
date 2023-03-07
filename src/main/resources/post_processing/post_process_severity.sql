-- Diagnosis stage
DO
$$
BEGIN
CREATE TEMP TABLE IF NOT EXISTS icdseverity AS
SELECT
	split_part(data_one, ':', 1) AS severity
	, NULL AS severity_id
	, split_part(data_one, ':', 2) AS severity_domain
	, 27 AS severity_domain_id
	, split_part(data_two, ':', 1) AS diagnosis_code
	, NULL AS diagnosis_code_id
	, split_part(data_two, ':', 2) AS diagnosis_domain
	, CASE
		WHEN split_part(data_two, ':', 2)= 'Condition' THEN 19
		WHEN split_part(data_two, ':', 2) = 'Measurement' THEN 21
		WHEN split_part(data_two, ':', 2)= 'Observation' THEN 27
		WHEN split_part(data_two, ':', 2)= 'Procedure' THEN 10
	END diagnosis_domain_id
	, fhir_logical_id
	, fhir_identifier
FROM
	cds_etl_helper.post_process_map
WHERE
	omop_table = 'severity';

CREATE INDEX severity ON icdseverity (severity, severity_id, severity_domain);

CREATE INDEX icd_code ON icdseverity (diagnosis_code, diagnosis_code_id, diagnosis_domain, diagnosis_domain_id);

CREATE INDEX fhir_logical_identifier_id ON icdseverity (fhir_logical_id, fhir_identifier);
END
$$;

DO
$$
BEGIN
-- severity_id from observation
WITH icd_severity AS (
SELECT
	DISTINCT "is".severity
	, "is".fhir_logical_id
	, "is".fhir_identifier
	, o.observation_id
FROM
	icdseverity "is"
JOIN observation o 
ON
	"is".severity = o.observation_source_value
	AND "is".fhir_logical_id = o.fhir_logical_id
	AND o.qualifier_concept_id = 4077563
)
UPDATE
	icdseverity
SET
	severity_id = icd_severity.observation_id
FROM
	icd_severity
WHERE
	icd_severity.fhir_logical_id = icdseverity.fhir_logical_id;
-- condition_occurrence
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, co.condition_occurrence_id
FROM
	icdseverity "is"
JOIN condition_occurrence co
ON
	"is".diagnosis_code = co.condition_source_value
	AND "is".fhir_logical_id = co.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 19
)
UPDATE
	icdseverity
SET
	diagnosis_code_id = icd_code.condition_occurrence_id
FROM
	icd_code
WHERE
	icdseverity.fhir_logical_id = icd_code.fhir_logical_id
	AND icdseverity.diagnosis_code = icd_code.diagnosis_code
	AND icdseverity.diagnosis_domain_id = 19;
-- procedure_occurrence
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, po.procedure_occurrence_id
FROM
	icdseverity "is"
JOIN procedure_occurrence po
ON
	"is".diagnosis_code = po.procedure_source_value
	AND "is".fhir_logical_id = po.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 10
)
UPDATE
	icdseverity
SET
	diagnosis_code_id = icd_code.procedure_occurrence_id
FROM
	icd_code
WHERE
	icdseverity.fhir_logical_id = icd_code.fhir_logical_id
	AND icdseverity.diagnosis_code = icd_code.diagnosis_code
	AND icdseverity.diagnosis_domain_id = 10;
-- measurement
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, m.measurement_id
FROM
	icdseverity "is"
JOIN measurement m
ON
	"is".diagnosis_code = m.value_source_value
	AND "is".fhir_logical_id = m.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 21
)
UPDATE
	icdseverity
SET
	diagnosis_code_id = icd_code.measurement_id
FROM
	icd_code
WHERE
	icdseverity.fhir_logical_id = icd_code.fhir_logical_id
	AND icdseverity.diagnosis_code = icd_code.diagnosis_code
	AND icdseverity.diagnosis_domain_id = 21;
-- observation
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, o.observation_id
FROM
	icdseverity "is"
JOIN observation o
ON
	"is".diagnosis_code = o.observation_source_value
	AND "is".fhir_logical_id = o.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 27
)
UPDATE
	icdseverity
SET
	diagnosis_code_id = icd_code.observation_id
FROM
	icd_code
WHERE
	icdseverity.fhir_logical_id = icd_code.fhir_logical_id
	AND icdseverity.diagnosis_code = icd_code.diagnosis_code
	AND icdseverity.diagnosis_domain_id = 27;
END
$$;

DO
$$
DECLARE v_rowCount INT;

BEGIN
WITH primary_insert AS
    (
INSERT
	INTO cds_cdm.fact_relationship(
		domain_concept_id_1
		, fact_id_1
		, domain_concept_id_2
		, fact_id_2
		, relationship_concept_id
		, fhir_logical_id_1
		, fhir_identifier_1
		, fhir_logical_id_2
		, fhir_identifier_2)
		SELECT
			diagnosis_domain_id
			, diagnosis_code_id::int
			, severity_domain_id
			, severity_id::int
			, 44818747
			, fhir_logical_id
			, fhir_identifier
			, fhir_logical_id
			, fhir_identifier
		FROM
			icdseverity
         RETURNING *
    )
INSERT
	INTO
	cds_cdm.fact_relationship
    (domain_concept_id_1
	, fact_id_1
	, domain_concept_id_2
	, fact_id_2
	, relationship_concept_id
	, fhir_logical_id_1
	, fhir_identifier_1
	, fhir_logical_id_2
	, fhir_identifier_2
    )
SELECT
	domain_concept_id_2
	, fact_id_2
	, domain_concept_id_1
	, fact_id_1
	, 44818845
	, fhir_logical_id_2
	, fhir_identifier_2
	, fhir_logical_id_1
	, fhir_identifier_1
FROM
	primary_insert;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;

RAISE NOTICE 'Affected % rows in fact_relationship for severity.', v_rowCount * 2;
END
$$;

DO
$$
BEGIN
DROP TABLE icdseverity;
END
$$;
