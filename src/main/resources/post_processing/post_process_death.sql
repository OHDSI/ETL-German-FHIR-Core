 -- upsert in death
DO
$$
DECLARE v_rowcount INT;
BEGIN
  WITH deathData AS (WITH deathInformation AS
  (
                  SELECT DISTINCT omop_id                                                             AS death_type_concept_id,
                                  To_date(data_one, 'YYYY-MM-DD')                                     AS death_date,
                                  To_timestamp(data_two, 'YYYY-MM-DD HH24:MI:SS')                     AS death_datetime,
                                  fhir_logical_id                                                     AS patient_id,
                                  fhir_identifier                                                     AS patient_identifier
                  FROM            cds_etl_helper.post_process_map
                  WHERE           omop_table = 'death')
  SELECT    *
  FROM      deathInformation di
  JOIN      person p
  ON        di.patient_id = p.fhir_logical_id),
   upsert AS
  (
         update death d
         SET    person_id = dd.person_id,
                death_date = dd.death_date,
                death_datetime = dd.death_datetime,
                death_type_concept_id = dd.death_type_concept_id,
                fhir_logical_id = dd.fhir_logical_id,
                fhir_identifier = dd.fhir_identifier
         FROM   deathData dd
         WHERE  d.person_id = dd.person_id returning d.* )
  INSERT INTO death
              (
                          person_id,
                          death_date,
                          death_datetime,
                          death_type_concept_id,
                          fhir_logical_id,
                          fhir_identifier
              )
  SELECT person_id,
         death_date,
         death_datetime,
         death_type_concept_id,
         fhir_logical_id,
         fhir_identifier
  FROM   deathData
  WHERE  NOT EXISTS
         (
                SELECT 1
                FROM   upsert
                WHERE  upsert.person_id = deathData.person_id);
  get diagnostics v_rowcount = ROW_COUNT;
  raise notice 'Upserted % rows in death.',v_rowcount;
END;
$$;
