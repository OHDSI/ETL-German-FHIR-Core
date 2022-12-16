-- upsert into location
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH post_locations AS
(
    SELECT DISTINCT
        SUBSTRING(Split_part(data_one, ';', 2) FOR 50) AS city
        , SUBSTRING(Split_part(data_one, ';', 1) FOR 9) AS zip
        , TRIM(SUBSTRING(concat(Split_part(data_one, ';', 1),' ',Split_part(data_one, ';', 2)) FOR 50)) AS source_value
        , SUBSTRING(Split_part(data_one, ';', 3) FOR 2) AS country
        , SUBSTRING(Split_part(data_two,';',1) FOR 50)  AS line1
        , SUBSTRING(Split_part(data_two,';',1),51,101)  AS line2
        , SUBSTRING(Split_part(data_two,';',2) FOR 20)  AS county
    FROM cds_etl_helper.post_process_map
    WHERE omop_table = 'LOCATION'
)
, new_locations AS
(
    SELECT
        pl.city
        , pl.zip
        , pl.source_value
        , pl.country
        , pl.line1
        , pl.line2
        , pl.county
    FROM post_locations pl
    LEFT JOIN cds_cdm.location l
           ON l.location_source_value = pl.source_value
    WHERE l.location_id IS NULL
)
INSERT INTO cds_cdm.location
    (
        city
        , zip
        , location_source_value
        , state
        , address_1
        , address_2
        , county
    )
SELECT
    city
    , zip
    , source_value
    , country
    , line1
    , line2
    , county
FROM
    new_locations
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Upserted % rows in location.',v_rowCount;
END;
$$;

-- update location_id in person table
DO
$$
DECLARE v_rowCount int;
BEGIN
WITH location_reference AS
(
    SELECT
        split_part(data_one,';',1) as zip
        , substring(split_part(data_one,';',2) for 50) as city
        , fhir_logical_id
        --, fhir_identifier
    FROM cds_etl_helper.post_process_map
    WHERE omop_table = 'LOCATION' AND type = 'PATIENT'
)
, reference AS
(
    SELECT
        lr.zip
        , lr.city
        , lr.fhir_logical_id
        --, lr.fhir_identifier
        , l.location_id
    FROM location_reference lr
    LEFT JOIN cds_cdm.location l
           ON l.city = lr.city
          AND l.zip = lr.zip
)
UPDATE person
   SET location_id = r.location_id
FROM reference r
WHERE (r.fhir_logical_id = person.fhir_logical_id)
   --OR (r.fhir_identifier = person.fhir_identifier)
;
GET DIAGNOSTICS v_rowCount = ROW_COUNT;
RAISE NOTICE 'Updated % rows in person.',v_rowCount;
END;
$$;
