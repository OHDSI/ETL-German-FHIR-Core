 -- upsert in death
DO
$$
DECLARE v_rowcount INT;
BEGIN
 WITH age_at_diagnosis AS
(
       SELECT ppm.data_one                   AS documentation_date,
              Split_part(ppm.data_two,':',1) AS age,
              Split_part(ppm.data_two,':',2) AS age_unit_source,
              Split_part(ppm.data_two,':',3) AS age_unit,
              ppm.omop_id                    AS aad_concept ,
              p.person_id,
              ppm.fhir_logical_id,
              ppm.fhir_identifier
       FROM   cds_etl_helper.post_process_map ppm
       JOIN   person p
       ON     ppm.fhir_logical_id =p.fhir_logical_id
       OR     ppm.fhir_identifier =p.fhir_identifier
       WHERE  omop_table ='age_at_diagnosis'), unit_concept AS
(
       SELECT aad.documentation_date::date,
              aad.age::               integer,
              aad.age_unit_source,
              aad.aad_concept,
              aad.person_id ,
              c.concept_id,
              aad.fhir_logical_id,
              aad.fhir_identifier
       FROM   age_at_diagnosis aad
       JOIN   concept c
       ON     c.concept_code =aad.age_unit
       WHERE  c.vocabulary_id='UCUM')
INSERT INTO observation
            (
                        observation_date,
                        value_as_number,
                        unit_source_value,
                        unit_concept_id,
                        observation_concept_id,
                        person_id,
                        observation_type_concept_id,
                        fhir_logical_id,
                        fhir_identifier
            )
SELECT documentation_date,
       age,
       age_unit_source,
       concept_id,
       aad_concept,
       person_id,
       32817,
       fhir_logical_id,
       fhir_identifier
FROM   unit_concept;
  get diagnostics v_rowcount = ROW_COUNT;
  raise notice 'Upserted % rows in observation.',v_rowcount;
END;
$$;
