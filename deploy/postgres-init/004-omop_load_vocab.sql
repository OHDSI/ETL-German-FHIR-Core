DO $$
DECLARE
  table_empty BOOLEAN;
BEGIN

  SELECT COUNT(*) = 0 INTO table_empty FROM cds_cdm.concept;

  IF table_empty THEN
    -- If the table is empty, then execute the COPY statements
    COPY cds_cdm.vocabulary FROM '/home/vocab/VOCABULARY.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.relationship FROM '/home/vocab/RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.concept_class FROM '/home/vocab/CONCEPT_CLASS.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.domain FROM '/home/vocab/DOMAIN.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.concept(concept_id,concept_name,domain_id,vocabulary_id,concept_class_id,standard_concept,concept_code,valid_start_date,valid_end_date,invalid_reason) FROM '/home/vocab/CONCEPT.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.concept_relationship FROM '/home/vocab/CONCEPT_RELATIONSHIP.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.concept_ancestor FROM '/home/vocab/CONCEPT_ANCESTOR.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.concept_synonym FROM '/home/vocab/CONCEPT_SYNONYM.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
    COPY cds_cdm.drug_strength FROM '/home/vocab/DRUG_STRENGTH.csv' WITH DELIMITER E'\t' CSV HEADER QUOTE E'\b' ;
  ELSE
    -- If the table is not empty, do nothing or handle it as needed
    RAISE NOTICE 'Table cds_cdm.concept is not empty, skipping COPY statements';
  END IF;
END;
$$;