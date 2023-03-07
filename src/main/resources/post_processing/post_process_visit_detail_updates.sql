--update column admitting_source_value
Do
$$
DECLARE v_rowCount int;
BEGIN
WITH fr AS
(
    SELECT
        visit_detail_id
        , lag(visit_detail_source_value, 1) OVER ( PARTITION BY person_id, visit_occurrence_id ORDER BY visit_detail_start_datetime) AS source_value
    FROM cds_cdm.visit_detail
)
UPDATE cds_cdm.visit_detail
SET admitting_source_value = fr.source_value
FROM fr
WHERE visit_detail.visit_detail_id = fr.visit_detail_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Update % rows for column admitting_source_value.',v_rowCount;
END;
$$;

--update column discharge_to_source_value
Do
$$
DECLARE v_rowCount int;
BEGIN
WITH discharge_to AS
(
    SELECT
        visit_detail_id
        , lead(visit_detail_source_value, 1) OVER (PARTITION BY person_id, visit_occurrence_id ORDER BY visit_detail_start_datetime) AS source_value
    FROM cds_cdm.visit_detail
)
UPDATE cds_cdm.visit_detail
SET discharge_to_source_value = discharge_to.source_value
FROM discharge_to
WHERE visit_detail.visit_detail_id=discharge_to.visit_detail_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Update % rows for column discharge_to_source_value.',v_rowCount;
END;
$$;

--update preceding_visit_detail_id
Do
$$
DECLARE v_rowCount int;
BEGIN
WITH id_order AS
(
    SELECT
        visit_detail_id
        , lag(visit_detail_id, 1) OVER (PARTITION BY person_id, visit_occurrence_id ORDER BY visit_detail_start_datetime) AS previous_visit_detail_id
   FROM cds_cdm.visit_detail
)
UPDATE cds_cdm.visit_detail
SET preceding_visit_detail_id = id_order.previous_visit_detail_id
FROM id_order
WHERE visit_detail.visit_detail_id=id_order.visit_detail_id
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Update % rows for column preceding_visit_detail_id.',v_rowCount;
END;
$$;

--read FK Constraints for precending_visit_detail_id
Do
$$
BEGIN
IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage WHERE constraint_name = 'fpk_v_detail_preceding')
THEN
ALTER TABLE visit_detail ADD CONSTRAINT fpk_v_detail_preceding FOREIGN KEY (preceding_visit_detail_id) REFERENCES visit_detail (visit_detail_id);
END IF;
END;
$$;

Do
$$
BEGIN
IF NOT EXISTS (SELECT constraint_name FROM information_schema.constraint_column_usage WHERE constraint_name = 'fpk_v_detail_parent')
THEN
ALTER TABLE visit_detail ADD CONSTRAINT fpk_v_detail_parent FOREIGN KEY (visit_detail_parent_id) REFERENCES visit_detail (visit_detail_id);
END IF;
END;
$$;
