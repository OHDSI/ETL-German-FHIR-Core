DO
$$
DECLARE v_rowCount int;
BEGIN
UPDATE cds_cdm.provider AS p
    SET care_site_id = por.care_site_id
    FROM (
        SELECT
            ppm.data_one AS practitioner_logical_id,
            ppm.data_two AS organization_logical_id,
            cs.care_site_id AS care_site_id
        FROM
            cds_etl_helper.post_process_map ppm
        JOIN cds_cdm.care_site cs ON cs.fhir_logical_id = ppm.data_two
        WHERE
            ppm.type = 'PRACTITIONERROLE'
    ) AS por
    WHERE p.fhir_logical_id = por.practitioner_logical_id;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in provider.',v_rowCount;
END;
$$;