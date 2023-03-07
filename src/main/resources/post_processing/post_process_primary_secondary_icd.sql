-- primary ICD and secondary ICD
DO
$$
BEGIN
CREATE TEMP TABLE IF NOT EXISTS icdpairs AS
SELECT
	split_part(data_one, ':', 1) AS primary_code
	, NULL AS primary_id
	, split_part(data_one, ':', 2) AS primary_domain
	, CASE
		WHEN split_part(data_one, ':', 2)= 'Condition' THEN 19
		WHEN split_part(data_one, ':', 2) = 'Measurement' THEN 21
		WHEN split_part(data_one, ':', 2)= 'Observation' THEN 27
		WHEN split_part(data_one, ':', 2)= 'Procedure' THEN 10
	END primary_domain_id
	, split_part(data_two, ':', 1) AS secondary_code
	, NULL AS secondary_id
	, split_part(data_two, ':', 2) AS secondary_domain
	, CASE
		WHEN split_part(data_two, ':', 2)= 'Condition' THEN 19
		WHEN split_part(data_two, ':', 2) = 'Measurement' THEN 21
		WHEN split_part(data_two, ':', 2)= 'Observation' THEN 27
		WHEN split_part(data_two, ':', 2)= 'Procedure' THEN 10
	END secondary_domain_id
	, fhir_logical_id
	, fhir_identifier
FROM
	cds_etl_helper.post_process_map
WHERE
	omop_table = 'primary_secondary_icd'
	AND omop_id = 0;

CREATE INDEX primary_icd_code ON icdpairs (primary_code, primary_id, primary_domain);

CREATE INDEX secondary_icd_code ON icdpairs (secondary_code, secondary_id, secondary_domain);

CREATE INDEX fhir_logical_identifier_id ON icdpairs (fhir_logical_id, fhir_identifier);

END
$$;

DO
$$
BEGIN

-- condition_occurrence
WITH primary_icds AS (
SELECT
	DISTINCT ip.*
	, co.condition_occurrence_id
FROM
	icdpairs ip
JOIN condition_occurrence co
ON
	ip.primary_code = co.condition_source_value
	AND (ip.fhir_logical_id = co.fhir_logical_id
		OR ip.fhir_logical_id = co.fhir_logical_id)
WHERE
	ip.primary_domain = 'Condition'
)
UPDATE
	icdpairs
SET
	primary_id = primary_icds.condition_occurrence_id
FROM
	primary_icds
WHERE
	icdpairs.primary_domain = 'Condition'
	AND
	primary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

WITH secondary_icds AS (
SELECT
	ip.*
	, co.condition_occurrence_id
FROM
	icdpairs ip
JOIN condition_occurrence co  
ON
	ip.secondary_code = co.condition_source_value
	AND (ip.fhir_logical_id = co.fhir_logical_id
		OR ip.fhir_logical_id = co.fhir_logical_id)
WHERE
	ip.secondary_domain = 'Condition'
)
UPDATE
	icdpairs
SET
	secondary_id = secondary_icds.condition_occurrence_id
FROM
	secondary_icds
WHERE
	icdpairs.secondary_domain = 'Condition'
	AND
	secondary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

--procedure
WITH primary_icds AS (
SELECT
	ip.*
	, po.procedure_occurrence_id
FROM
	icdpairs ip
JOIN procedure_occurrence po
ON
	ip.primary_code = po.procedure_source_value
	AND (ip.fhir_logical_id = po.fhir_logical_id
		OR ip.fhir_logical_id = po.fhir_logical_id)
WHERE
	ip.primary_domain = 'Procedure'
)
UPDATE
	icdpairs
SET
	primary_id = primary_icds.procedure_occurrence_id
FROM
	primary_icds
WHERE
	icdpairs.primary_domain = 'Procedure'
	AND
	primary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

WITH secondary_icds AS (
SELECT
	ip.*
	, po.procedure_occurrence_id
FROM
	icdpairs ip
JOIN procedure_occurrence po
ON
	ip.secondary_code = po.procedure_source_value
	AND (ip.fhir_logical_id = po.fhir_logical_id
		OR ip.fhir_logical_id = po.fhir_logical_id)
WHERE
	ip.secondary_domain = 'Procedure'
)
UPDATE
	icdpairs
SET
	secondary_id = secondary_icds.procedure_occurrence_id
FROM
	secondary_icds
WHERE
	icdpairs.secondary_domain = 'Procedure'
	AND
	secondary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

--observation
WITH primary_icds AS (
SELECT
	DISTINCT ip.*
	, o.observation_id
FROM
	icdpairs ip
JOIN observation o
ON
	ip.primary_code = o.observation_source_value
	AND (ip.fhir_logical_id = o.fhir_logical_id
		OR ip.fhir_logical_id = o.fhir_logical_id)
WHERE
	ip.primary_domain = 'Observation'
)
UPDATE
	icdpairs
SET
	primary_id = primary_icds.observation_id
FROM
	primary_icds
WHERE
	icdpairs.primary_domain = 'Observation'
	AND
	primary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

WITH secondary_icds AS (
SELECT
	ip.*
	, o.observation_id
FROM
	icdpairs ip
JOIN observation o
ON
	ip.secondary_code = o.observation_source_value
	AND (ip.fhir_logical_id = o.fhir_logical_id
		OR ip.fhir_logical_id = o.fhir_logical_id)
WHERE
	ip.secondary_domain = 'Observation'
)
UPDATE
	icdpairs
SET
	secondary_id = secondary_icds.observation_id
FROM
	secondary_icds
WHERE
	icdpairs.secondary_domain = 'Observation'
	AND
	secondary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

--measurement
WITH primary_icds AS (
SELECT
	ip.*
	, m.measurement_id
FROM
	icdpairs ip
JOIN measurement m
ON
	ip.primary_code = m.measurement_source_value
	AND (ip.fhir_logical_id = m.fhir_logical_id
		OR ip.fhir_logical_id = m.fhir_logical_id)
WHERE
	ip.primary_domain = 'Measurement'
)
UPDATE
	icdpairs
SET
	primary_id = primary_icds.measurement_id
FROM
	primary_icds
WHERE
	icdpairs.primary_domain = 'Measurement'
	AND
	primary_icds.fhir_logical_id = icdpairs.fhir_logical_id;

WITH secondary_icds AS (
SELECT
	ip.*
	, m.measurement_id
FROM
	icdpairs ip
JOIN measurement m
ON
	ip.secondary_code = m.measurement_source_value
	AND (ip.fhir_logical_id = m.fhir_logical_id
		OR ip.fhir_logical_id = m.fhir_logical_id)
WHERE
	ip.secondary_domain = 'Measurement'
)
UPDATE
	icdpairs
SET
	secondary_id = secondary_icds.measurement_id
FROM
	secondary_icds
WHERE
	icdpairs.secondary_domain = 'Measurement'
	AND
	secondary_icds.fhir_logical_id = icdpairs.fhir_logical_id;
END
$$;

-- insert and update
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH primary_insert AS (
INSERT
	INTO
		fact_relationship (
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
			primary_domain_id::int
			, primary_id::int
			, secondary_domain_id::int
			, secondary_id::int
			, 44818770
			, fhir_logical_id
			, fhir_identifier
			, fhir_logical_id
			, fhir_identifier
		FROM
			icdpairs
		WHERE
			primary_id IS NOT NULL
			AND secondary_id IS NOT NULL
		RETURNING *
)
,
secondary_insert AS (
INSERT
	INTO
		cds_cdm.fact_relationship
(
    domain_concept_id_1
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
			, 44818868
			, fhir_logical_id_2
			, fhir_identifier_2
			, fhir_logical_id_1
			, fhir_identifier_1
		FROM
			primary_insert
RETURNING *
)
UPDATE
	cds_etl_helper.post_process_map
SET
	omop_id = 1
FROM
	secondary_insert ir
WHERE
	omop_table = 'primary_secondary_icd'
AND
	fhir_logical_id = ir.fhir_logical_id_1;

GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for primary and secondary ICD.',v_rowCount*2;
END
$$;

DO
$$
BEGIN
DROP TABLE icdpairs;
END
$$;
