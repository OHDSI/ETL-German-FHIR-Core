-- Update diagnosis_use for condition_occurrence
DO $$
DECLARE
  updated_omop_table_rowcount INT;
BEGIN
WITH update_con_occ AS
(
       update condition_occurrence co
       SET    condition_status_source_value = split_part(uip.data_two, ':', 1),
              condition_status_concept_id = split_part(uip.data_two, ':', 2)::int
       FROM   cds_etl_helper.post_process_map uip
       WHERE  omop_table = 'use'
       AND    omop_id = 0
       AND    (
                     split_part(uip.data_one, ':con-', 1) = co.fhir_logical_id
              OR     'con-'
                            || split_part(uip.data_one, ':con-', 2) = co.fhir_identifier)
       AND    (
                     condition_status_source_value IS NULL
              OR     condition_status_source_value IN ( 'AD',
                                                       'DD',
                                                       'CC',
                                                       'CM',
                                                       'pre-op',
                                                       'post-op',
                                                       'billing')) returning co.*), update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 1
       FROM   update_con_occ ucc
       WHERE  omop_table = 'use'
       AND    omop_id = 0
       AND    (
                     split_part(uip.data_one, ':con-', 1) = ucc.fhir_logical_id
              OR     ucc.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(uco.*)  AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   update_con_occ uco;

  raise notice 'Updated % rows in condition_occurrence for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;

-- insert row of diagosis_use in condition_occurrence
DO $$

DECLARE
  updated_omop_table_rowcount INT;

BEGIN
WITH diagnose_use AS
(
       SELECT co.*,
              ppm.data_one ,
              ppm.data_two ,
              ppm.omop_table ,
              ppm.omop_id
       FROM   condition_occurrence co,
              cds_etl_helper.post_process_map ppm
       WHERE  ppm.omop_table ='use'
       AND    omop_id =0
       AND    (
                     co.fhir_logical_id = Split_part(ppm.data_one, ':con-', 1)
              OR     co.fhir_identifier ='con-'
                            || Split_part(ppm.data_one, ':con-', 2))
       AND    (
                     condition_status_source_value IS NOT NULL
              AND    condition_status_source_value NOT IN ( 'AD',
                                                           'DD',
                                                           'CC',
                                                           'CM',
                                                           'pre-op',
                                                           'post-op',
                                                           'billing'))) ,inserted_rows AS
(
            insert INTO condition_occurrence
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
            SELECT DISTINCT du.person_id,
                   du.condition_concept_id,
                   du.condition_start_date,
                   du.condition_start_datetime,
                   du.condition_end_date,
                   du.condition_end_datetime,
                   du.condition_type_concept_id,
                   du.stop_reason,
                   du.provider_id,
                   du.visit_occurrence_id,
                   du.visit_detail_id,
                   du.condition_source_value,
                   du.condition_source_concept_id,
                   split_part(du.data_two, ':', 1),
                   split_part(du.data_two, ':', 2)::integer,
                   du.fhir_logical_id,
                   du.fhir_identifier
            FROM   diagnose_use du returning condition_occurrence.*) ,update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 1
       FROM   inserted_rows ir
       WHERE  omop_table = 'use'
       AND    omop_id = 0
       AND    (
                     split_part(uip.data_one, ':con-', 1) = ir.fhir_logical_id
              OR     ir.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(ir.*)   AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   inserted_rows ir;

  raise notice 'Inserted % rows in condition_occurrence for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;

-- Update diagnosis_use for procedure_occurrence
DO $$

DECLARE
  updated_omop_table_rowcount INT;
  updated_ppm_rowcount INT;

BEGIN
WITH update_pro_occ AS
(
       update procedure_occurrence po
       SET    modifier_source_value = split_part(uip.data_two, ':', 1),
              modifier_concept_id = split_part(uip.data_two, ':', 2)::int
       FROM   cds_etl_helper.post_process_map uip
       WHERE  omop_table = 'use'
       AND    omop_id <= 1
       AND    (
                     split_part(uip.data_one, ':con-', 1) = po.fhir_logical_id
              OR     'con-'
                            || split_part(uip.data_one, ':con-', 2) = po.fhir_identifier)
       AND    (
                     modifier_source_value IS NULL
              OR     modifier_source_value IN ( 'AD',
                                               'DD',
                                               'CC',
                                               'CM',
                                               'pre-op',
                                               'post-op',
                                               'billing')) returning po.*), update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 2
       FROM   update_pro_occ upc
       WHERE  omop_table = 'use'
       AND    omop_id <= 1
       AND    (
                     split_part(uip.data_one, ':con-', 1) = upc.fhir_logical_id
              OR     upc.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(upo.*)  AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   update_pro_occ upo;

  raise notice 'Updated % rows in procedure_occurrence for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;


-- insert row of diagosis_use in procedure_occurrence
DO $$

DECLARE
  updated_omop_table_rowcount INT;

BEGIN
WITH diagnose_use AS
(
       SELECT po.*,
              ppm.data_one ,
              ppm.data_two ,
              ppm.omop_table ,
              ppm.omop_id
       FROM   procedure_occurrence po,
              cds_etl_helper.post_process_map ppm
       WHERE  ppm.omop_table ='use'
       AND    omop_id <=1
       AND    (
                     po.fhir_logical_id = Split_part(ppm.data_one, ':con-', 1)
              OR     po.fhir_identifier ='con-'
                            || Split_part(ppm.data_one, ':con-', 2))
       AND    (
                     modifier_source_value IS NOT NULL
              AND    modifier_source_value NOT IN ( 'AD',
                                                   'DD',
                                                   'CC',
                                                   'CM',
                                                   'pre-op',
                                                   'post-op',
                                                   'billing'))) ,inserted_rows AS
(
            insert INTO procedure_occurrence
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
            SELECT DISTINCT du.person_id,
                   du.procedure_concept_id,
                   du.procedure_date,
                   du.procedure_datetime,
                   du.procedure_type_concept_id,
                   split_part(du.data_two, ':', 2)::integer,
                   du.quantity,
                   du.provider_id,
                   du.visit_occurrence_id,
                   du.visit_detail_id,
                   du.procedure_source_value,
                   du.procedure_source_concept_id,
                   split_part(du.data_two, ':', 1),
                   du.fhir_logical_id,
                   du.fhir_identifier
            FROM   diagnose_use du returning procedure_occurrence.*) ,update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 2
       FROM   inserted_rows ir
       WHERE  omop_table = 'use'
       AND    omop_id <= 1
       AND    (
                     split_part(uip.data_one, ':con-', 1) = ir.fhir_logical_id
              OR     ir.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(ir.*)   AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   inserted_rows ir;

  raise notice 'Inserted % rows in procedure_occurrence for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;


-- Update diagnosis_use for measurement
DO $$

DECLARE
  updated_omop_table_rowcount INT;
BEGIN
WITH update_measurement AS
(
       update measurement m
       SET    value_source_value = split_part(uip.data_two, ':', 1),
              value_as_concept_id = split_part(uip.data_two, ':', 2)::int
       FROM   cds_etl_helper.post_process_map uip
       WHERE  omop_table = 'use'
       AND    omop_id <= 2
       AND    (
                     split_part(uip.data_one, ':con-', 1) = m.fhir_logical_id
              OR     'con-'
                            || split_part(uip.data_one, ':con-', 2) = m.fhir_identifier)
       AND    (
                     value_source_value IS NULL
              OR     value_source_value IN ( 'AD',
                                            'DD',
                                            'CC',
                                            'CM',
                                            'pre-op',
                                            'post-op',
                                            'billing')) returning m.*), update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 3
       FROM   update_measurement um
       WHERE  omop_table = 'use'
       AND    omop_id <= 2
       AND    (
                     split_part(uip.data_one, ':con-', 1) = um.fhir_logical_id
              OR     um.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(um.*)   AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   update_measurement um;

  raise notice 'Updated % rows in measurement for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;


-- insert row of diagosis_use in measurement
DO $$

DECLARE
  updated_omop_table_rowcount INT;

BEGIN
WITH diagnose_use AS
(
       SELECT m.*,
              ppm.data_one ,
              ppm.data_two ,
              ppm.omop_table ,
              ppm.omop_id
       FROM   measurement m,
              cds_etl_helper.post_process_map ppm
       WHERE  ppm.omop_table ='use'
       AND    omop_id <=2
       AND    (
                     m.fhir_logical_id = Split_part(ppm.data_one, ':con-', 1)
              OR     m.fhir_identifier ='con-'
                            || Split_part(ppm.data_one, ':con-', 2))
       AND    (
                     value_source_value IS NOT NULL
              AND    value_source_value NOT IN ( 'AD',
                                                'DD',
                                                'CC',
                                                'CM',
                                                'pre-op',
                                                'post-op',
                                                'billing'))) ,inserted_rows AS
(
            insert INTO measurement
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
            SELECT DISTINCT du.person_id,
                   du.measurement_concept_id,
                   du.measurement_date,
                   du.measurement_datetime,
                   du.measurement_time,
                   du.measurement_type_concept_id,
                   du.operator_concept_id,
                   du.value_as_number,
                   split_part(du.data_two, ':', 2)::integer,
                   du.unit_concept_id,
                   du.range_low,
                   du.range_high,
                   du.provider_id,
                   du.visit_occurrence_id,
                   du.visit_detail_id,
                   du.measurement_source_value,
                   du.measurement_source_concept_id,
                   du.unit_source_value,
                   split_part(du.data_two, ':', 1),
                   du.fhir_logical_id,
                   du.fhir_identifier
            FROM   diagnose_use du returning measurement.*) ,update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 3
       FROM   inserted_rows ir
       WHERE  omop_table = 'use'
       AND    omop_id <= 2
       AND    (
                     split_part(uip.data_one, ':con-', 1) = ir.fhir_logical_id
              OR     ir.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(ir.*)   AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   inserted_rows ir;

  raise notice 'Inserted % rows in measurement for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;


-- Update diagnosis_use for observation
DO $$

DECLARE
  updated_omop_table_rowcount INT;
  updated_ppm_rowcount INT;

BEGIN
WITH update_observation AS
(
       update observation o
       SET    qualifier_source_value = split_part(uip.data_two, ':', 1),
              qualifier_concept_id = split_part(uip.data_two, ':', 2)::int
       FROM   cds_etl_helper.post_process_map uip
       WHERE  omop_table = 'use'
       AND    omop_id <= 3
       AND    (
                     split_part(uip.data_one, ':con-', 1) = o.fhir_logical_id
              OR     'con-'
                            || split_part(uip.data_one, ':con-', 2) = o.fhir_identifier)
       AND    value_as_string IS NULL
       AND    (
                     qualifier_source_value IS NULL
              OR     qualifier_source_value IN ( 'AD',
                                                'DD',
                                                'CC',
                                                'CM',
                                                'pre-op',
                                                'post-op',
                                                'billing')) returning o.*), update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 4
       FROM   update_observation uo
       WHERE  omop_table = 'use'
       AND    omop_id <= 3
       AND    (
                     split_part(uip.data_one, ':con-', 1) = uo.fhir_logical_id
              OR     uo.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(uo.*)   AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   update_observation uo;

  raise notice 'Updated % rows in observation for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;


-- insert row of diagosis_use in observation
DO $$

DECLARE
  updated_omop_table_rowcount INT;
  updated_ppm_rowcount INT;

BEGIN
WITH diagnose_use AS
(
       SELECT o.*,
              ppm.data_one ,
              ppm.data_two ,
              ppm.omop_table ,
              ppm.omop_id
       FROM   observation o,
              cds_etl_helper.post_process_map ppm
       WHERE  ppm.omop_table ='use'
       AND    omop_id <=3
       AND    o.value_as_string IS NULL
       AND    (
                     o.fhir_logical_id = Split_part(ppm.data_one, ':con-', 1)
              OR     o.fhir_identifier ='con-'
                            || Split_part(ppm.data_one, ':con-', 2))
       AND    (
                     qualifier_source_value IS NOT NULL
              AND    qualifier_source_value NOT IN ( 'AD',
                                                    'DD',
                                                    'CC',
                                                    'CM',
                                                    'pre-op',
                                                    'post-op',
                                                    'billing'))) ,inserted_rows AS
(
            insert INTO observation
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
            SELECT DISTINCT du.person_id,
                   du.observation_concept_id,
                   du.observation_date,
                   du.observation_datetime,
                   du.observation_type_concept_id,
                   du.value_as_number,
                   du.value_as_string,
                   du.value_as_concept_id,
                   split_part(du.data_two, ':', 2)::integer,
                   du.unit_concept_id,
                   du.provider_id,
                   du.visit_occurrence_id,
                   du.visit_detail_id,
                   du.observation_source_value,
                   du.observation_source_concept_id,
                   du.unit_source_value,
                   split_part(du.data_two, ':', 1),
                   du.fhir_logical_id,
                   du.fhir_identifier
            FROM   diagnose_use du returning observation.*) ,update_post_process_map AS
(
       UPDATE cds_etl_helper.post_process_map uip
       SET    omop_id = 4
       FROM   inserted_rows ir
       WHERE  omop_table = 'use'
       AND    omop_id <= 3
       AND    (
                     split_part(uip.data_one, ':con-', 1) = ir.fhir_logical_id
              OR     ir.fhir_identifier = 'con-'
                            || split_part(uip.data_one, ':con-', 2)) returning 1 )
SELECT count(ir.*)   AS row_counts_updated_table
INTO   updated_omop_table_rowcount
FROM   inserted_rows ir;

  raise notice 'Inserted % rows in observation for "diagnose_use".', updated_omop_table_rowcount;
END;
$$;

DO $$

BEGIN

UPDATE cds_etl_helper.post_process_map SET omop_id =4 WHERE omop_table ='use' AND omop_id>0;

END;
$$;
