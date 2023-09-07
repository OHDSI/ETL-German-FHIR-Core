-- ICD site localization
DO
$$
BEGIN
CREATE TEMP TABLE IF NOT EXISTS icdlocalizations AS
SELECT
	split_part(data_one, ':', 1) AS localization_code
	, NULL AS localization_obs_id
	, split_part(data_one, ':', 2) AS localization_domain
	, 27 AS localization_domain_id
	, split_part(data_two, ':', 1) AS icd_code
	, NULL AS icd_code_id
	, split_part(data_two, ':', 2) AS icd_code_domain
	, CASE
		WHEN split_part(data_two, ':', 2)= 'Condition' THEN 19
		WHEN split_part(data_two, ':', 2) = 'Measurement' THEN 21
		WHEN split_part(data_two, ':', 2)= 'Observation' THEN 27
		WHEN split_part(data_two, ':', 2)= 'Procedure' THEN 10
	END icd_code_domain_id
	, fhir_logical_id
	, fhir_identifier
FROM
	cds_etl_helper.post_process_map
WHERE
	omop_table = 'site_localization';

CREATE INDEX localization_code ON icdlocalizations (localization_code, localization_obs_id, localization_domain, localization_domain_id);

CREATE INDEX icd_code ON icdlocalizations (icd_code, icd_code_id, icd_code_domain, icd_code_domain_id);

CREATE INDEX fhir_logical_identifier_id ON icdlocalizations (fhir_logical_id, fhir_identifier);

END
$$;

DO
$$
BEGIN
-- site localization
WITH localizations AS (
SELECT
	DISTINCT il.fhir_logical_id
	, o.observation_id
	, il.localization_code
FROM
	icdlocalizations il
JOIN observation o
ON
	il.fhir_logical_id = o.fhir_logical_id
	AND il.localization_code = o.value_as_string
)
UPDATE
	icdlocalizations
SET
	localization_obs_id = localizations.observation_id
FROM
	localizations
WHERE
	localizations.fhir_logical_id = icdlocalizations.fhir_logical_id;

-- condition_occurrence
WITH icd_codes AS (
SELECT
	DISTINCT
	il.fhir_logical_id
	, co.condition_occurrence_id
FROM
	icdlocalizations il
JOIN condition_occurrence co
ON
	il.icd_code = co.condition_source_value
	AND (il.fhir_logical_id = co.fhir_logical_id
		OR il.fhir_logical_id = co.fhir_logical_id)
WHERE
	il.icd_code_domain = 'Condition'
)
UPDATE
	icdlocalizations
SET
	icd_code_id = icd_codes.condition_occurrence_id
FROM
	icd_codes
WHERE
	icdlocalizations.icd_code_domain = 'Condition'
AND
	icd_codes.fhir_logical_id = icdlocalizations.fhir_logical_id;

-- procedure_occurrence
WITH icd_codes AS (
SELECT
	DISTINCT
	il.fhir_logical_id
	, po.procedure_occurrence_id
FROM
	icdlocalizations il
JOIN procedure_occurrence po
ON
	il.icd_code = po.procedure_source_value
	AND (il.fhir_logical_id = po.fhir_logical_id
		OR il.fhir_logical_id = po.fhir_logical_id)
WHERE
	il.icd_code_domain = 'Procedure'
)
UPDATE
	icdlocalizations
SET
	icd_code_id = icd_codes.procedure_occurrence_id
FROM
	icd_codes
WHERE
	icdlocalizations.icd_code_domain = 'Procedure'
AND
	icd_codes.fhir_logical_id = icdlocalizations.fhir_logical_id;

-- observation
WITH icd_codes AS (
SELECT
	DISTINCT
	il.fhir_logical_id
	, o.observation_id
FROM
	icdlocalizations il
JOIN observation o
ON
	il.icd_code = o.observation_source_value
	AND (il.fhir_logical_id = o.fhir_logical_id
		OR il.fhir_logical_id = o.fhir_logical_id)
WHERE
	il.icd_code_domain = 'Observation'
)
UPDATE
	icdlocalizations
SET
	icd_code_id = icd_codes.observation_id
FROM
	icd_codes
WHERE
	icdlocalizations.icd_code_domain = 'Observation'
AND
	icd_codes.fhir_logical_id = icdlocalizations.fhir_logical_id;

-- measurement
WITH icd_codes AS (
SELECT
	DISTINCT
	il.fhir_logical_id
	, m.measurement_id
FROM
	icdlocalizations il
JOIN measurement m
ON
	il.icd_code = m.measurement_source_value
	AND (il.fhir_logical_id = m.fhir_logical_id
		OR il.fhir_logical_id = m.fhir_logical_id)
WHERE
	il.icd_code_domain = 'Measurement'
)
UPDATE
	icdlocalizations
SET
	icd_code_id = icd_codes.measurement_id
FROM
	icd_codes
WHERE
	icdlocalizations.icd_code_domain = 'Measurement'
AND
	icd_codes.fhir_logical_id = icdlocalizations.fhir_logical_id;
END
$$;

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
			icd_code_domain_id
			, icd_code_id::int
			, localization_domain_id
			, localization_obs_id::int
			, 44818762
			, fhir_logical_id
			, fhir_identifier
			, fhir_logical_id
			, fhir_identifier
		FROM
			icdlocalizations
		WHERE
			icd_code_id IS NOT NULL
			AND localization_obs_id IS NOT NULL
		RETURNING *
)
INSERT
	INTO cds_cdm.fact_relationship
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
	, 44818860
	, fhir_logical_id_2
	, fhir_identifier_2
	, fhir_logical_id_1
	, fhir_identifier_1
FROM
	primary_insert;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in fact_relationship for ICD site localization.',v_rowCount*2;
END
$$;

DO
$$
BEGIN
DROP TABLE icdlocalizations;
END
$$;
