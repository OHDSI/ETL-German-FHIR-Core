-- Update diagnosis_rank for condition_occurrence
DO $$
DECLARE
  v_rowcount INT;
BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id =0 ), exists_con_occ AS
  (
            SELECT    *
            FROM      condition_occurrence co
            LEFT JOIN rank_in_ppm rip
            ON        (
                                rip.diagnose_fhir_id = co.fhir_logical_id
                      OR        rip.diagnose_fhir_identifier = co.fhir_identifier)
            WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL) , updated_rows AS
  (
         UPDATE  condition_occurrence
         SET    condition_status_source_value=ecs.diagnose_status_source_value_in_ppm,
                condition_status_concept_id= ecs.diagnose_status_concept_id_in_ppm
         FROM   exists_con_occ ecs
         WHERE  (
                       ecs.diagnose_fhir_id = condition_occurrence.fhir_logical_id
                OR     ecs.diagnose_fhir_identifier = condition_occurrence.fhir_identifier)
         AND    ecs.diagnose_status_source_value_in_ppm IS NOT NULL
         AND    (
                       condition_occurrence.condition_status_source_value IS NULL
                OR     (condition_occurrence.condition_status_source_value ~ '^[0-9.]+$' AND condition_occurrence.condition_status_source_value::integer > 0)) returning condition_occurrence.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =1
  FROM   updated_rows ur
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ur.fhir_logical_id;

  get diagnostics v_rowcount = row_count;
  raise notice 'Updated % rows in condition_occurrence.', v_rowcount;
END;
$$;

-- insert row of diagosis_rank in condition_occurrence
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id =0 ) , inserted_rows AS
  (
              INSERT INTO condition_occurrence
                          (
                                      person_id,
                                      condition_concept_id,
                                      condition_start_date,
                                      condition_start_datetime,
                                      condition_end_date,
                                      condition_end_datetime,
                                      condition_type_concept_id,
                                      stop_reason,
                                      provider_id,
                                      visit_occurrence_id,
                                      visit_detail_id,
                                      condition_source_value,
                                      condition_source_concept_id,
                                      condition_status_source_value,
                                      condition_status_concept_id,
                                      fhir_logical_id,
                                      fhir_identifier
                          )
              SELECT  DISTINCT  ecs.person_id,
                        ecs.condition_concept_id,
                        ecs.condition_start_date,
                        ecs.condition_start_datetime,
                        ecs.condition_end_date,
                        ecs.condition_end_datetime,
                        ecs.condition_type_concept_id,
                        ecs.stop_reason,
                        ecs.provider_id,
                        ecs.visit_occurrence_id,
                        ecs.visit_detail_id,
                        ecs.condition_source_value,
                        ecs.condition_source_concept_id,
                        rip.diagnose_status_source_value_in_ppm,
                        rip.diagnose_status_concept_id_in_ppm,
                        ecs.fhir_logical_id,
                        ecs.fhir_identifier
              FROM      condition_occurrence ecs
              LEFT JOIN rank_in_ppm rip
              ON        (
                                  rip.diagnose_fhir_id = ecs.fhir_logical_id
                        OR        rip.diagnose_fhir_identifier = ecs.fhir_identifier)
              WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL
              AND       ecs.condition_status_source_value NOT LIKE '^[0-9.]+$' returning condition_occurrence.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =1
  FROM   inserted_rows ir
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ir.fhir_logical_id;

  get diagnostics v_rowcount = row_count;
  raise notice 'Inserted % rows in condition_occurrence.', v_rowcount;
END;
$$;

-- Update diagnosis_rank for procedure_occurrence
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id <=1 ), exists_con_occ AS
  (
            SELECT    *
            FROM      procedure_occurrence po
            LEFT JOIN rank_in_ppm rip
            ON        (
                                rip.diagnose_fhir_id = po.fhir_logical_id
                      OR        rip.diagnose_fhir_identifier = po.fhir_identifier)
            WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL ), updated_rows AS
  (
         UPDATE procedure_occurrence
         SET    modifier_source_value = ecs.diagnose_status_source_value_in_ppm,
                modifier_concept_id = ecs.diagnose_status_concept_id_in_ppm
         FROM   exists_con_occ ecs
         WHERE  (
                       ecs.diagnose_fhir_id = procedure_occurrence.fhir_logical_id
                OR     ecs.diagnose_fhir_identifier = procedure_occurrence.fhir_identifier)
         AND    (
                       procedure_occurrence.modifier_source_value IS NULL
                OR     (procedure_occurrence.modifier_source_value ~ '^[0-9.]+$' AND procedure_occurrence.modifier_source_value::integer > 0)) returning procedure_occurrence.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =2
  FROM   updated_rows ur
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ur.fhir_logical_id;

  GET DIAGNOSTICS v_rowcount = row_count;
  RAISE NOTICE 'Updated % rows in procedure_occurrence.', v_rowcount;
END;
$$;

-- insert row of diagosis_rank in procedure_occurrence
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id <=1 ), inserted_rows AS
  (
              INSERT INTO procedure_occurrence
                          (
                                      person_id,
                                      procedure_concept_id,
                                      procedure_date,
                                      procedure_datetime,
                                      procedure_type_concept_id,
                                      modifier_concept_id,
                                      quantity,
                                      provider_id,
                                      visit_occurrence_id,
                                      visit_detail_id,
                                      procedure_source_value,
                                      procedure_source_concept_id,
                                      modifier_source_value,
                                      fhir_logical_id,
                                      fhir_identifier
                          )
              SELECT  DISTINCT  ecs.person_id,
                        ecs.procedure_concept_id,
                        ecs.procedure_date,
                        ecs.procedure_datetime,
                        ecs.procedure_type_concept_id,
                        rip.diagnose_status_concept_id_in_ppm,
                        ecs.quantity,
                        ecs.provider_id,
                        ecs.visit_occurrence_id,
                        ecs.visit_detail_id,
                        ecs.procedure_source_value,
                        ecs.procedure_source_concept_id,
                        rip.diagnose_status_source_value_in_ppm,
                        ecs.fhir_logical_id,
                        ecs.fhir_identifier
              FROM      procedure_occurrence ecs
              LEFT JOIN rank_in_ppm rip
              ON        (
                                  rip.diagnose_fhir_id = ecs.fhir_logical_id
                        OR        rip.diagnose_fhir_identifier = ecs.fhir_identifier)
              WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL
              AND       ecs.modifier_source_value NOT LIKE '^[0-9.]+$' returning procedure_occurrence.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =2
  FROM   inserted_rows ir
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ir.fhir_logical_id;

  GET DIAGNOSTICS v_rowcount = row_count;
  RAISE NOTICE 'Inserted % rows in procedure_occurrence.', v_rowcount;
END;
$$;

-- Update diagnosis_rank for measurement
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id <=2 ), exists_con_occ AS
  (
            SELECT    *
            FROM      measurement m
            LEFT JOIN rank_in_ppm rip
            ON        (
                                rip.diagnose_fhir_id = m.fhir_logical_id
                      OR        rip.diagnose_fhir_identifier = m.fhir_identifier)
            WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL ), updated_rows AS
  (
         UPDATE measurement
         SET    value_source_value = ecs.diagnose_status_source_value_in_ppm,
                value_as_concept_id = ecs.diagnose_status_concept_id_in_ppm
         FROM   exists_con_occ ecs
         WHERE  (
                       ecs.diagnose_fhir_id = measurement.fhir_logical_id
                OR     ecs.diagnose_fhir_identifier = measurement.fhir_identifier)
         AND    (
                       measurement.value_source_value IS NULL
                OR     (measurement.value_source_value ~ '^[0-9.]+$' AND cast(measurement.value_source_value AS DOUBLE PRECISION) > 0))returning measurement.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =3
  FROM   updated_rows ur
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ur.fhir_logical_id;

  get diagnostics v_rowcount = row_count;
  raise notice 'Updated % rows in measurement.', v_rowcount;
END;
$$;

-- insert row of diagosis_rank in measurement
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id <=2 ), inserted_rows AS
  (
              INSERT INTO measurement
                          (
                                      person_id,
                                      measurement_concept_id,
                                      measurement_date,
                                      measurement_datetime,
                                      measurement_time,
                                      measurement_type_concept_id,
                                      operator_concept_id,
                                      value_as_number,
                                      value_as_concept_id,
                                      unit_concept_id,
                                      range_low,
                                      range_high,
                                      provider_id,
                                      visit_occurrence_id,
                                      visit_detail_id,
                                      measurement_source_value,
                                      measurement_source_concept_id,
                                      unit_source_value,
                                      value_source_value,
                                      fhir_logical_id,
                                      fhir_identifier
                          )
              SELECT  DISTINCT  ecs.person_id,
                        ecs.measurement_concept_id,
                        ecs.measurement_date,
                        ecs.measurement_datetime,
                        ecs.measurement_time,
                        ecs.measurement_type_concept_id,
                        ecs.operator_concept_id,
                        ecs.value_as_number,
                        rip.diagnose_status_concept_id_in_ppm,
                        ecs.unit_concept_id,
                        ecs.range_low,
                        ecs.range_high,
                        ecs.provider_id,
                        ecs.visit_occurrence_id,
                        ecs.visit_detail_id,
                        ecs.measurement_source_value,
                        ecs.measurement_source_concept_id,
                        ecs.unit_source_value,
                        rip.diagnose_status_source_value_in_ppm,
                        ecs.fhir_logical_id,
                        ecs.fhir_identifier
              FROM      measurement ecs
              LEFT JOIN rank_in_ppm rip
              ON        (
                                  rip.diagnose_fhir_id = ecs.fhir_logical_id
                        OR        rip.diagnose_fhir_identifier = ecs.fhir_identifier)
              WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL
              AND       ecs.value_source_value NOT LIKE '^[0-9.]+$' returning measurement.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =3
  FROM   inserted_rows ir
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ir.fhir_logical_id;

  get diagnostics v_rowcount = row_count;
  raise notice 'Inserted % rows in measurement.', v_rowcount;
END;
$$;

-- Update diagnosis_rank for observation
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id <=3 ), exists_con_occ AS
  (
            SELECT    *
            FROM      observation o
            LEFT JOIN rank_in_ppm rip
            ON        (
                                rip.diagnose_fhir_id = o.fhir_logical_id
                      OR        rip.diagnose_fhir_identifier = o.fhir_identifier)
            WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL ), updated_rows AS
  (
         UPDATE observation
         SET    qualifier_source_value = ecs.diagnose_status_source_value_in_ppm,
                qualifier_concept_id = ecs.diagnose_status_concept_id_in_ppm
         FROM   exists_con_occ ecs
         WHERE  (
                       ecs.diagnose_fhir_id = observation.fhir_logical_id
                OR     ecs.diagnose_fhir_identifier = observation.fhir_identifier )
         AND observation.value_as_string is null
         AND    (
                       observation.qualifier_source_value IS NULL
                OR     (observation.qualifier_source_value ~ '^[0-9.]+$' AND cast(observation.qualifier_source_value AS DOUBLE PRECISION) > 0))returning observation.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =4
  FROM   updated_rows ur
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ur.fhir_logical_id;

  get diagnostics v_rowcount = row_count;
  raise notice 'Updated % rows in observation.', v_rowcount;
END;
$$;

-- insert row of diagosis_rank in observation
DO $$

DECLARE v_rowcount INT;

BEGIN
  WITH rank_in_ppm AS
  (
         SELECT Split_part(ppm.data_one, ':con-', 1) AS diagnose_fhir_id,
                'con-'
                       || Split_part(ppm.data_one, ':con-', 2) AS diagnose_fhir_identifier,
                Split_part(ppm.data_two, ':', 1)               AS diagnose_status_source_value_in_ppm,
                Split_part(ppm.data_two, ':', 2)::integer      AS diagnose_status_concept_id_in_ppm,
                omop_id                                        AS is_written
         FROM   cds_etl_helper.post_process_map ppm
         WHERE  omop_table = 'rank'
         AND    omop_id <=3 ), inserted_rows AS
  (
              INSERT INTO observation
                          (
                                      person_id,
                                      observation_concept_id,
                                      observation_date,
                                      observation_datetime,
                                      observation_type_concept_id,
                                      value_as_number,
                                      value_as_string,
                                      value_as_concept_id,
                                      qualifier_concept_id,
                                      unit_concept_id,
                                      provider_id,
                                      visit_occurrence_id,
                                      visit_detail_id,
                                      observation_source_value,
                                      observation_source_concept_id,
                                      unit_source_value,
                                      qualifier_source_value,
                                      fhir_logical_id,
                                      fhir_identifier
                          )
              SELECT  DISTINCT  ecs.person_id,
                        ecs.observation_concept_id,
                        ecs.observation_date,
                        ecs.observation_datetime,
                        ecs.observation_type_concept_id,
                        ecs.value_as_number,
                        ecs.value_as_string,
                        ecs.value_as_concept_id,
                        rip.diagnose_status_concept_id_in_ppm,
                        ecs.unit_concept_id,
                        ecs.provider_id,
                        ecs.visit_occurrence_id,
                        ecs.visit_detail_id,
                        ecs.observation_source_value,
                        ecs.observation_source_concept_id,
                        ecs.unit_source_value,
                        rip.diagnose_status_source_value_in_ppm,
                        ecs.fhir_logical_id,
                        ecs.fhir_identifier
              FROM      observation ecs
              LEFT JOIN rank_in_ppm rip
              ON        (
                                  rip.diagnose_fhir_id = ecs.fhir_logical_id
                        OR        rip.diagnose_fhir_identifier = ecs.fhir_identifier)
              WHERE     rip.diagnose_status_source_value_in_ppm IS NOT NULL
              AND       ecs.qualifier_source_value NOT LIKE '^[0-9.]+$' returning observation.*)
  UPDATE cds_etl_helper.post_process_map
  SET    omop_id =4
  FROM   inserted_rows ir
  WHERE  omop_table ='rank'
  AND    Split_part(data_one, ':con-', 1)=ir.fhir_logical_id;

  get diagnostics v_rowcount = row_count;
  raise notice 'Inserted % rows in observation.', v_rowcount;
END;
$$;

DO $$

BEGIN

UPDATE cds_etl_helper.post_process_map SET omop_id =4 WHERE omop_table ='rank' AND omop_id > 0;

END;
$$;
