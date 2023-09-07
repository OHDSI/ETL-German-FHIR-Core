-- Diagnosis stage
DO
$$
BEGIN
CREATE TEMP TABLE IF NOT EXISTS icdstage AS
SELECT
	split_part(data_one, ':', 1) AS stage
	, NULL AS stage_id
	, split_part(data_one, ':', 2) AS stage_domain
	, 27 AS stage_domain_id
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
	omop_table = 'stage';

CREATE INDEX stage ON icdstage (stage, stage_id, stage_domain);

CREATE INDEX icd_code ON icdstage (diagnosis_code, diagnosis_code_id, diagnosis_domain, diagnosis_domain_id);

CREATE INDEX fhir_logical_identifier_id ON icdstage (fhir_logical_id, fhir_identifier);
END
$$;

DO
$$
BEGIN
-- stage_id from observation
WITH icd_stage AS (
SELECT
	DISTINCT "is".stage
	, "is".fhir_logical_id
	, "is".fhir_identifier
	, o.observation_id
FROM
	icdstage "is"
JOIN observation o
ON
	"is".stage = o.observation_source_value
	AND "is".fhir_logical_id = o.fhir_logical_id
	AND o.qualifier_concept_id = 4106767
)
UPDATE
	icdstage
SET
	stage_id = icd_stage.observation_id
FROM
	icd_stage
WHERE
	icd_stage.fhir_logical_id = icdstage.fhir_logical_id;

-- condition_occurrence
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, co.condition_occurrence_id
FROM
	icdstage "is"
JOIN condition_occurrence co
ON
	"is".diagnosis_code = co.condition_source_value
	AND "is".fhir_logical_id = co.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 19
)
UPDATE
	icdstage
SET
	diagnosis_code_id = icd_code.condition_occurrence_id
FROM
	icd_code
WHERE
	icdstage.fhir_logical_id = icd_code.fhir_logical_id
	AND icdstage.diagnosis_code = icd_code.diagnosis_code
	AND icdstage.diagnosis_domain_id = 19;

-- procedure_occurrence
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, po.procedure_occurrence_id
FROM
	icdstage "is"
JOIN procedure_occurrence po
ON
	"is".diagnosis_code = po.procedure_source_value
	AND "is".fhir_logical_id = po.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 10
)
UPDATE
	icdstage
SET
	diagnosis_code_id = icd_code.procedure_occurrence_id
FROM
	icd_code
WHERE
	icdstage.fhir_logical_id = icd_code.fhir_logical_id
	AND icdstage.diagnosis_code = icd_code.diagnosis_code
	AND icdstage.diagnosis_domain_id = 10;

-- measurement
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, m.measurement_id
FROM
	icdstage "is"
JOIN measurement m
ON
	"is".diagnosis_code = m.value_source_value
	AND "is".fhir_logical_id = m.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 21
)
UPDATE
	icdstage
SET
	diagnosis_code_id = icd_code.measurement_id
FROM
	icd_code
WHERE
	icdstage.fhir_logical_id = icd_code.fhir_logical_id
	AND icdstage.diagnosis_code = icd_code.diagnosis_code
	AND icdstage.diagnosis_domain_id = 21;

-- observation
WITH icd_code AS (
SELECT
	"is".fhir_logical_id
	, "is".diagnosis_code
	, o.observation_id
FROM
	icdstage "is"
JOIN observation o
ON
	"is".diagnosis_code = o.observation_source_value
	AND "is".fhir_logical_id = o.fhir_logical_id
WHERE
	"is".diagnosis_domain_id = 27
)
UPDATE
	icdstage
SET
	diagnosis_code_id = icd_code.observation_id
FROM
	icd_code
WHERE
	icdstage.fhir_logical_id = icd_code.fhir_logical_id
	AND icdstage.diagnosis_code = icd_code.diagnosis_code
	AND icdstage.diagnosis_domain_id = 27;
END
$$;

DO
$$
DECLARE v_rowCount INT;
BEGIN
WITH primary_insert AS
    (
INSERT
	INTO
		cds_cdm.fact_relationship(
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
			, stage_domain_id
			, stage_id::int
			, 45754814
			, fhir_logical_id
			, fhir_identifier
			, fhir_logical_id
			, fhir_identifier
		FROM
			icdstage
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
	, 45754815
	, fhir_logical_id_2
	, fhir_identifier_2
	, fhir_logical_id_1
	, fhir_identifier_1
FROM
	primary_insert;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for stage.',v_rowCount*2;
END
$$;

DO
$$
BEGIN
DROP TABLE icdstage;
END
$$;
