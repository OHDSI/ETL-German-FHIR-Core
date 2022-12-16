--for condition_occurrence
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH unique_visit_detail AS
(
    SELECT
        Array_agg(vd.visit_detail_id) AS visit_detail_id
        , co.condition_occurrence_id
    FROM cds_cdm.visit_detail vd
    INNER JOIN cds_cdm.condition_occurrence co
            ON co.person_id=vd.person_id
           AND vd.visit_detail_end_datetime >= COALESCE(co.condition_end_datetime, co.condition_start_datetime)
           AND vd.visit_detail_start_datetime <= co.condition_start_datetime
    GROUP BY co.condition_occurrence_id
    HAVING ( COUNT(vd.visit_detail_id)=1 )
)
UPDATE cds_cdm.condition_occurrence co
   SET visit_detail_id=c.visit_detail_id[1]::integer
FROM unique_visit_detail c
WHERE c.condition_occurrence_id=co.condition_occurrence_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in condition_occurrence.',v_rowCount;
END;
$$;

--for procedure_occurrence
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH unique_visit_detail AS
(
    SELECT
        Array_agg(vd.visit_detail_id) AS visit_detail_id
        , po.procedure_occurrence_id
    FROM cds_cdm.visit_detail vd
    INNER JOIN cds_cdm.procedure_occurrence po
            ON po.person_id=vd.person_id
            AND po.procedure_datetime <= vd.visit_detail_end_datetime
            AND po.procedure_datetime >= vd.visit_detail_start_datetime
    GROUP BY po.procedure_occurrence_id
    HAVING ( COUNT(vd.visit_detail_id)=1 )
)
UPDATE cds_cdm.procedure_occurrence po
    SET visit_detail_id = c.visit_detail_id[1]::integer
FROM unique_visit_detail c
WHERE c.procedure_occurrence_id=po.procedure_occurrence_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in procedure_occurrence.',v_rowCount;
END;
$$;

--for measurement
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH unique_visit_detail AS
(
    SELECT
        Array_agg(vd.visit_detail_id) AS visit_detail_id
        , m.measurement_id
    FROM cds_cdm.visit_detail vd
    INNER JOIN cds_cdm.measurement m
            ON m.person_id=vd.person_id
           AND m.measurement_datetime<=vd.visit_detail_end_datetime
           AND m.measurement_datetime >=vd.visit_detail_start_datetime
           AND m.visit_detail_id IS NULL
    GROUP BY m.measurement_id
    HAVING(COUNT(vd.visit_detail_id)=1)
)
UPDATE cds_cdm.measurement m
SET    visit_detail_id=c.visit_detail_id[1]::integer
FROM   unique_visit_detail c
WHERE c.measurement_id=m.measurement_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in measurement.',v_rowCount;
END;
$$;

--for observation
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH unique_visit_detail AS
(
    SELECT
        Array_agg(vd.visit_detail_id) AS visit_detail_id
        , o.observation_id
    FROM cds_cdm.visit_detail vd
    INNER JOIN cds_cdm.observation o
            ON o.person_id=vd.person_id
            AND o.observation_datetime <= vd.visit_detail_end_datetime
            AND o.observation_datetime >= vd.visit_detail_start_datetime
    GROUP BY o.observation_id
    HAVING ( COUNT(vd.visit_detail_id)=1 )
)
UPDATE cds_cdm.observation o
SET    visit_detail_id=c.visit_detail_id[1]::integer
FROM   unique_visit_detail c
WHERE c.observation_id=o.observation_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in observation.',v_rowCount;
END;
$$;

--for drug_exposure
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH unique_visit_detail AS
(
    SELECT
        Array_agg(vd.visit_detail_id) AS visit_detail_id
        , de.drug_exposure_id
    FROM cds_cdm.visit_detail vd
    INNER JOIN cds_cdm.drug_exposure de
            ON de.person_id = vd.person_id
           AND vd.visit_detail_end_datetime >= COALESCE(de.drug_exposure_end_datetime, de.drug_exposure_start_datetime)
           AND vd.visit_detail_start_datetime <= de.drug_exposure_start_datetime
    GROUP BY de.drug_exposure_id
    HAVING ( COUNT(vd.visit_detail_id)=1 )
)
UPDATE cds_cdm.drug_exposure de
SET    visit_detail_id=c.visit_detail_id[1]::integer
FROM   unique_visit_detail c
WHERE c.drug_exposure_id=de.drug_exposure_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in drug_exposure.',v_rowCount;
END;
$$;
