DO
$$
DECLARE v_rowCount int;
BEGIN
WITH post_process AS
(
	SELECT
        omop_id
        , TO_DATE(min(data_one), 'YYYY-MM-DD') AS startDate
		, TO_DATE(max(data_two), 'YYYY-MM-DD') AS endDate
		, 32817 AS period_type_concept_id
	FROM cds_etl_helper.post_process_map
	WHERE type = 'ENCOUNTER'
	  AND omop_table = 'observation_period'
	GROUP BY omop_id
	)
, upsert AS
(
	UPDATE cds_cdm.observation_period op
	SET observation_period_start_date =
            CASE
                WHEN observation_period_start_date > pp.startDate
                THEN pp.startDate
                ELSE observation_period_start_date
			END
        , observation_period_end_date =
            CASE
                WHEN observation_period_end_date < pp.endDate
				THEN pp.endDate
                ELSE observation_period_end_date
			END
	FROM post_process pp
	WHERE pp.omop_id = op.person_id RETURNING op.*
)
INSERT INTO cds_cdm.observation_Period
(
	person_id
	, observation_period_start_date
	, observation_period_end_date
	, period_type_concept_id
)
SELECT
    omop_id
    , startDate
    , endDate
    , period_type_concept_id
FROM post_process
WHERE NOT EXISTS
    (
        SELECT
            1
		FROM upsert
		WHERE upsert.person_id = post_process.omop_id
    );
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Affected % rows in observation_period.',v_rowCount;
END;
$$;
