-- Clean up the fact_relationship table
DO
$$
DECLARE v_rowcount INT; final_rowcount INT;
BEGIN
-- Deleting entities for condition_occurrence
WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_1
	, fr.fhir_logical_id_1
FROM
	fact_relationship fr
LEFT JOIN condition_occurrence co
ON
	fr.fact_id_1 = co.condition_occurrence_id
WHERE
	fr.domain_concept_id_1 = 19
	AND co.condition_occurrence_id IS NULL)
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.domain_concept_id_1 = 19
	AND fr2.fact_id_1 = to_delete.fact_id_1 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_2
	, fr.fhir_logical_id_2
FROM
	fact_relationship fr
LEFT JOIN condition_occurrence co 
ON
	fr.fact_id_2 = co.condition_occurrence_id
WHERE
	fr.domain_concept_id_2 = 19
	AND co.condition_occurrence_id IS NULL)
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.domain_concept_id_2 = 19
	AND fr2.fact_id_2 = to_delete.fact_id_2 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

-- Deleting entities for procedure_occurrence
WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_1
	, fr.fhir_logical_id_1
FROM
	fact_relationship fr
LEFT JOIN procedure_occurrence po  
ON
	fr.fact_id_1 = po.procedure_occurrence_id
	AND fr.fhir_logical_id_1 = po.fhir_logical_id
WHERE
	fr.domain_concept_id_1 = 10
	AND po.procedure_occurrence_id IS NULL )
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.fact_id_1 = to_delete.fact_id_1
	AND fr2.fhir_logical_id_1 = to_delete.fhir_logical_id_1
	AND fr2.domain_concept_id_1 = 10 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_2
	, fr.fhir_logical_id_2
FROM
	fact_relationship fr
LEFT JOIN procedure_occurrence po  
ON
	fr.fact_id_2 = po.procedure_occurrence_id
	AND fr.fhir_logical_id_2 = po.fhir_logical_id
WHERE
	fr.domain_concept_id_2 = 10
	AND po.procedure_occurrence_id IS NULL )
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.fact_id_2 = to_delete.fact_id_2
	AND fr2.fhir_logical_id_2 = to_delete.fhir_logical_id_2
	AND fr2.domain_concept_id_2 = 10 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

-- Deleting entities for measurement
WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_1
	, fr.fhir_logical_id_1
FROM
	fact_relationship fr
LEFT JOIN measurement m  
ON
	fr.fact_id_1 = m.measurement_id
	AND fr.fhir_logical_id_1 = m.fhir_logical_id
WHERE
	fr.domain_concept_id_1 = 21
	AND m.measurement_id IS NULL )
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.fact_id_1 = to_delete.fact_id_1
	AND fr2.fhir_logical_id_1 = to_delete.fhir_logical_id_1
	AND fr2.domain_concept_id_1 = 21 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_2
	, fr.fhir_logical_id_2
FROM
	fact_relationship fr
LEFT JOIN measurement m  
ON
	fr.fact_id_2 = m.measurement_id
	AND fr.fhir_logical_id_2 = m.fhir_logical_id
WHERE
	fr.domain_concept_id_2 = 21
	AND m.measurement_id IS NULL )
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.fact_id_2 = to_delete.fact_id_2
	AND fr2.fhir_logical_id_2 = to_delete.fhir_logical_id_2
	AND fr2.domain_concept_id_2 = 21 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

-- Deleting entities for observation

WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_1
	, fr.fhir_logical_id_1
FROM
	fact_relationship fr
LEFT JOIN observation o  
ON
	fr.fact_id_1 = o.observation_id
	AND fr.fhir_logical_id_1 = o.fhir_logical_id
WHERE
	fr.domain_concept_id_1 = 27
	AND o.observation_id IS NULL )
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.fact_id_1 = to_delete.fact_id_1
	AND fr2.fhir_logical_id_1 = to_delete.fhir_logical_id_1
	AND fr2.domain_concept_id_1 = 27 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;

WITH to_delete AS (
SELECT
	DISTINCT fr.fact_id_2
	, fr.fhir_logical_id_2
FROM
	fact_relationship fr
LEFT JOIN observation o  
ON
	fr.fact_id_2 = o.observation_id
	AND fr.fhir_logical_id_2 = o.fhir_logical_id
WHERE
	fr.domain_concept_id_2 = 27
	AND o.observation_id IS NULL )
, deleted AS(
DELETE
FROM
	fact_relationship fr2
		USING to_delete
WHERE
	fr2.fact_id_2 = to_delete.fact_id_2
	AND fr2.fhir_logical_id_2 = to_delete.fhir_logical_id_2
	AND fr2.domain_concept_id_2 = 27 RETURNING 1)
SELECT
		count(*)
	FROM
		deleted
INTO
	v_rowcount;
final_rowcount=v_rowcount;
final_rowcount=final_rowcount+v_rowcount;

RAISE NOTICE 'Deleted % rows in fact_relationship.',final_rowcount;
END
$$;
