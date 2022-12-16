-- upsert in observation for admission reason
DO
$$
DECLARE v_rowcount INT;
BEGIN
WITH adReasonCodes AS
(
    SELECT DISTINCT
        omop_id AS pp_person_id
        , To_date(data_one, 'YYYY-MM-DD') AS observation_date
        , To_timestamp(data_one, 'YYYY-MM-DD HH24:MI:SS') AS observation_datetime
        , 32819 AS observation_type_concept_id
        , data_two AS value_as_string
        , 44803020 AS qualifier_concept_id
        , data_two AS observation_source_value
        , fhir_logical_id AS encounter_id
        , fhir_identifier AS encounter_identifier
    FROM cds_etl_helper.post_process_map
    WHERE omop_table = 'admission_reason'
)
, adReasonConcepts AS
(
    SELECT
        target_concept_id AS observation_concept_id
        , source_code
    FROM source_to_concept_map
    WHERE source_vocabulary_id = 'Admission Reason 1&2'
)
, adReasons AS
(
    SELECT
        ar.pp_person_id
        , ar.observation_date
        , ar.observation_datetime
        , ar.observation_type_concept_id
        , ar.value_as_string
        , ar.qualifier_concept_id
        , ar.observation_source_value
        , arc.observation_concept_id
        , vo.visit_occurrence_id
        , vo.fhir_logical_id
        , vo.fhir_identifier
    FROM adReasonCodes ar
    INNER JOIN adReasonConcepts arc
            ON ar.observation_source_value = arc.source_code
    INNER JOIN cds_cdm.visit_occurrence vo
            ON ar.encounter_id = vo.fhir_logical_id
)
, upsert AS
(
     UPDATE cds_cdm.observation ob
        SET observation_source_value = ar.observation_source_value
            , value_as_string = ar.value_as_string
            , observation_concept_id = ar.observation_concept_id
     FROM adReasons ar
     WHERE ob.observation_type_concept_id = ar.observation_type_concept_id
       AND ob.visit_occurrence_id = ar.visit_occurrence_id
       AND ob.qualifier_concept_id = ar.qualifier_concept_id
     RETURNING ob.*
)
INSERT INTO cds_cdm.observation
(
    person_id
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_string
    , qualifier_concept_id
    , visit_occurrence_id
    , observation_source_value
    , fhir_logical_id
    , fhir_identifier
)
SELECT
    pp_person_id
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_string
    , qualifier_concept_id
    , visit_occurrence_id
    , observation_source_value
    , fhir_logical_id
    , fhir_identifier
FROM adReasons
WHERE NOT EXISTS
    (
        SELECT
            1
        FROM upsert
        WHERE upsert.observation_type_concept_id = adReasons.observation_type_concept_id
          AND upsert.visit_occurrence_id = adReasons.visit_occurrence_id
          AND upsert.qualifier_concept_id = adReasons.qualifier_concept_id
     )
;
  get diagnostics v_rowcount = row_count;
  raise notice 'Upserted % rows in observation.',v_rowcount;
END;
$$;

-- upsert in observation for admission occasion
DO
$$
DECLARE v_rowcount INT;
BEGIN
WITH adOccasionCodes AS
(
    SELECT DISTINCT
        omop_id AS pp_person_id
        , To_date(data_one, 'YYYY-MM-DD') AS observation_date
        , To_timestamp(data_one, 'YYYY-MM-DD HH24:MI:SS') AS observation_datetime
        , 32817 AS observation_type_concept_id
        , data_two AS value_as_string
        , 44803020 AS qualifier_concept_id
        , data_two AS observation_source_value
        , fhir_logical_id AS encounter_id
        , fhir_identifier AS encounter_identifier
    FROM cds_etl_helper.post_process_map
    WHERE omop_table = 'admission_occasion'
)
, adOccasionConcepts AS
(
    SELECT
        target_concept_id AS observation_concept_id
        , source_code
    FROM source_to_concept_map
    WHERE source_vocabulary_id = 'Ad Occasion Obs'
)
, adOccasions AS
(
    SELECT
        ao.pp_person_id
        , ao.observation_date
        , ao.observation_datetime
        , ao.observation_type_concept_id
        , ao.value_as_string
        , ao.qualifier_concept_id
        , ao.observation_source_value
        , ao.encounter_id
        , ao.encounter_identifier
        , aoc.observation_concept_id
        , aoc.source_code
        , vo.visit_occurrence_id
        , vo.fhir_logical_id
        , vo.fhir_identifier
    FROM adOccasionCodes ao
    INNER JOIN adOccasionConcepts aoc
            ON ao.observation_source_value = aoc.source_code
    INNER JOIN cds_cdm.visit_occurrence vo
            ON ao.encounter_id = vo.fhir_logical_id
)
, upsert AS
(
    UPDATE cds_cdm.observation ob
       SET
        observation_source_value = ao.observation_source_value
        , value_as_string = ao.value_as_string
        , observation_concept_id = ao.observation_concept_id
    FROM adOccasions ao
    WHERE ob.observation_type_concept_id = ao.observation_type_concept_id
      AND ob.visit_occurrence_id = ao.visit_occurrence_id
      AND ob.qualifier_concept_id = ao.qualifier_concept_id
    RETURNING ob.*
)
INSERT INTO cds_cdm.observation
(
    person_id
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_string
    , qualifier_concept_id
    , visit_occurrence_id
    , observation_source_value
    , fhir_logical_id
    , fhir_identifier
)
SELECT
    pp_person_id
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_string
    , qualifier_concept_id
    , visit_occurrence_id
    , observation_source_value
    , fhir_logical_id
    , fhir_identifier
FROM adOccasions
WHERE NOT EXISTS
    (
        SELECT
            1
        FROM upsert
        WHERE upsert.observation_type_concept_id = adOccasions.observation_type_concept_id
          AND upsert.visit_occurrence_id = adOccasions.visit_occurrence_id
          AND upsert.qualifier_concept_id = adOccasions.qualifier_concept_id
    )
;
  get diagnostics v_rowcount = row_count;
  raise notice 'Upserted % rows in observation.',v_rowcount;
END;
$$;

-- upsert in observation for discharge reason
DO
$$
DECLARE v_rowcount INT;
BEGIN
  WITH disReasonCodes AS
(
    SELECT DISTINCT
        omop_id                                           AS pp_person_id
        , To_date(data_one, 'YYYY-MM-DD')                 AS observation_date
        , To_timestamp(data_one, 'YYYY-MM-DD HH24:MI:SS') AS observation_datetime
        , 32823                                           AS observation_type_concept_id
        , data_two                                        AS value_as_string
        , data_two                                        AS observation_source_value
        , SUBSTRING(data_two, 1, 2)                       AS dis_reason_code
        , fhir_logical_id                                 AS encounter_id
        , fhir_identifier                                 AS encounter_identifier
    FROM cds_etl_helper.post_process_map
    WHERE omop_table = 'discharge_reason'
)
, disReasonConcepts AS
(
    SELECT
        target_concept_id AS observation_concept_id
        , source_code
    FROM cds_cdm.source_to_concept_map
    WHERE source_vocabulary_id = 'Dis Reason 1&2 Obs'
)
, disReasons AS
(
    SELECT
        dr.pp_person_id
        , dr.observation_date
        , dr.observation_datetime
        , dr.observation_type_concept_id
        , dr.value_as_string
        , dr.observation_source_value
        , dr.encounter_id
        , dr.encounter_identifier
        , drc.observation_concept_id
        , drc.source_code
        , vo.visit_occurrence_id
        , vo.fhir_logical_id
        , vo.fhir_identifier
    FROM disReasonCodes dr
    INNER JOIN disReasonConcepts drc
            ON dr.dis_reason_code = drc.source_code
    INNER JOIN cds_cdm.visit_occurrence vo
            ON dr.encounter_id = vo.fhir_logical_id
)
, upsert AS
(
    UPDATE
        cds_cdm.observation ob
    SET
        observation_source_value = dr.observation_source_value
        , value_as_string = dr.value_as_string
        , observation_concept_id = dr.observation_concept_id
        , observation_date = dr.observation_date
        , observation_datetime = dr.observation_datetime
    FROM
        disReasons dr
    WHERE
        ob.observation_type_concept_id = dr.observation_type_concept_id
    AND ob.visit_occurrence_id = dr.visit_occurrence_id RETURNING ob.*
)
INSERT INTO cds_cdm.observation
    (
        person_id
        , observation_concept_id
        , observation_date
        , observation_datetime
        , observation_type_concept_id
        , value_as_string
        , visit_occurrence_id
        , observation_source_value
        , fhir_logical_id
        , fhir_identifier
    )
SELECT
    pp_person_id
    , observation_concept_id
    , observation_date
    , observation_datetime
    , observation_type_concept_id
    , value_as_string
    , visit_occurrence_id
    , observation_source_value
    , fhir_logical_id
    , fhir_identifier
FROM disReasons
WHERE NOT EXISTS
    (
        SELECT
            1
        FROM upsert
        WHERE upsert.observation_type_concept_id = disReasons.observation_type_concept_id
          AND upsert.visit_occurrence_id = disReasons.visit_occurrence_id
     )
;
  get diagnostics v_rowcount = row_count;
  raise notice 'Upserted % rows in observation.',v_rowcount;
END;
$$;
