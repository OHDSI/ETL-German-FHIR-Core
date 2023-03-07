--Change foriegn keys for person_id to 'on delete cascade'
DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_procedure_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.procedure_occurrence DROP CONSTRAINT fpk_procedure_person;
ALTER TABLE cds_cdm.procedure_occurrence ADD CONSTRAINT fpk_procedure_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_drug_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.drug_exposure DROP CONSTRAINT fpk_drug_person;
ALTER TABLE cds_cdm.drug_exposure ADD CONSTRAINT fpk_drug_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_observation_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.observation DROP CONSTRAINT fpk_observation_person;
ALTER TABLE cds_cdm.observation ADD CONSTRAINT fpk_observation_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_condition_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.condition_occurrence DROP CONSTRAINT fpk_condition_person;
ALTER TABLE cds_cdm.condition_occurrence ADD CONSTRAINT fpk_condition_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_visit_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.visit_occurrence DROP CONSTRAINT fpk_visit_person;
ALTER TABLE cds_cdm.visit_occurrence ADD CONSTRAINT fpk_visit_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_v_detail_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.visit_detail DROP CONSTRAINT fpk_v_detail_person;
ALTER TABLE cds_cdm.visit_detail ADD CONSTRAINT fpk_v_detail_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_observation_period_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.observation_period DROP CONSTRAINT fpk_observation_period_person;
ALTER TABLE cds_cdm.observation_period ADD CONSTRAINT fpk_observation_period_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_measurement_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.measurement DROP CONSTRAINT fpk_measurement_person;
ALTER TABLE cds_cdm.measurement ADD CONSTRAINT fpk_measurement_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_death_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.death DROP CONSTRAINT fpk_death_person;
ALTER TABLE cds_cdm.death ADD CONSTRAINT fpk_death_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_device_person'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.device_exposure DROP CONSTRAINT fpk_device_person;
ALTER TABLE cds_cdm.device_exposure ADD CONSTRAINT fpk_device_person FOREIGN KEY (person_id) REFERENCES cds_cdm.person(person_id) ON
DELETE
	CASCADE;
END IF;
END
$$;


--Change foriegn keys for visit_occurrence_id to 'on delete set NULL'
DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_procedure_visit'
	AND confdeltype = 'n')
	THEN
ALTER TABLE cds_cdm.procedure_occurrence DROP CONSTRAINT fpk_procedure_visit;
ALTER TABLE cds_cdm.procedure_occurrence ADD CONSTRAINT fpk_procedure_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id)
ON DELETE SET NULL;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_drug_visit'
	AND confdeltype = 'n')
	THEN
ALTER TABLE cds_cdm.drug_exposure DROP CONSTRAINT fpk_drug_visit;
ALTER TABLE cds_cdm.drug_exposure ADD CONSTRAINT fpk_drug_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id)
ON DELETE SET NULL;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_observation_visit'
	AND confdeltype = 'n')
	THEN
ALTER TABLE cds_cdm.observation DROP CONSTRAINT fpk_observation_visit;
ALTER TABLE cds_cdm.observation ADD CONSTRAINT fpk_observation_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id)
ON DELETE SET NULL;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_condition_visit'
	AND confdeltype = 'n')
	THEN
ALTER TABLE cds_cdm.condition_occurrence DROP CONSTRAINT fpk_condition_visit;
ALTER TABLE cds_cdm.condition_occurrence ADD CONSTRAINT fpk_condition_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id)
ON DELETE SET NULL;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpd_v_detail_visit'
	AND confdeltype = 'c')
	THEN
ALTER TABLE cds_cdm.visit_detail DROP CONSTRAINT fpd_v_detail_visit;
ALTER TABLE cds_cdm.visit_detail ADD CONSTRAINT fpd_v_detail_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id) ON
DELETE
	CASCADE;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_measurement_visit'
	AND confdeltype = 'n')
	THEN
ALTER TABLE cds_cdm.measurement DROP CONSTRAINT fpk_measurement_visit;
ALTER TABLE cds_cdm.measurement ADD CONSTRAINT fpk_measurement_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id)
ON DELETE SET NULL;
END IF;
END
$$;

DO
$$
BEGIN
IF NOT EXISTS (
SELECT
	conname
	, confdeltype
FROM
	pg_catalog.pg_constraint
JOIN pg_catalog.pg_class c ON
	c."oid" = conrelid
JOIN pg_catalog.pg_class p ON
	p."oid" = confrelid
WHERE
	conname = 'fpk_device_person'
	AND confdeltype = 'n')
	THEN
ALTER TABLE cds_cdm.device_exposure DROP CONSTRAINT fpk_device_visit;
ALTER TABLE cds_cdm.device_exposure ADD CONSTRAINT fpk_device_visit FOREIGN KEY (visit_occurrence_id) REFERENCES cds_cdm.visit_occurrence(visit_occurrence_id)
ON DELETE SET NULL;
END IF;
END
$$;
