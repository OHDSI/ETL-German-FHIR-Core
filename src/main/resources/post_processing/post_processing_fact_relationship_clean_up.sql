-- Clean up the fact_relationship table
DO
$$
DECLARE v_rowcount INT; final_rowcount INT;
BEGIN
DELETE FROM fact_relationship fr
WHERE  (fr.domain_concept_id_1 = 19
         AND fr.fact_id_1 NOT IN (SELECT condition_occurrence_id FROM condition_occurrence))
        OR  (fr.domain_concept_id_2 = 19
             AND fr.fact_id_2 NOT IN (SELECT condition_occurrence_id FROM condition_occurrence));
             GET DIAGNOSTICS v_rowcount = ROW_COUNT;
             final_rowcount=v_rowcount;

DELETE FROM fact_relationship fr
WHERE  (fr.domain_concept_id_1 = 27
         AND fr.fact_id_1 NOT IN (SELECT observation_id FROM observation))
        OR  (fr.domain_concept_id_2 = 27
             AND fr.fact_id_2 NOT IN (SELECT observation_id FROM observation));
             GET DIAGNOSTICS v_rowcount = ROW_COUNT;
             final_rowcount=final_rowcount+v_rowcount;

DELETE FROM fact_relationship fr
WHERE  (fr.domain_concept_id_1 = 21
         AND fr.fact_id_1 NOT IN (SELECT measurement_id FROM measurement))
        OR  (fr.domain_concept_id_2 = 21
             AND fr.fact_id_2 NOT IN (SELECT measurement_id FROM measurement));
             GET DIAGNOSTICS v_rowcount = ROW_COUNT;
             final_rowcount=final_rowcount+v_rowcount;

DELETE FROM fact_relationship fr
WHERE  (fr.domain_concept_id_1 = 10
         AND fr.fact_id_1 NOT IN (SELECT procedure_occurrence_id FROM procedure_occurrence))
        OR  (fr.domain_concept_id_2 = 10
             AND fr.fact_id_2 NOT IN (SELECT procedure_occurrence_id FROM procedure_occurrence));
  GET DIAGNOSTICS v_rowcount = ROW_COUNT;
  final_rowcount=final_rowcount+v_rowcount;
  raise notice 'Deleted % rows in fact_relationship.',final_rowcount;
END;
$$;
