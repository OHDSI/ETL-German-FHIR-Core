--Alter drug_exposure table to automatically increment drug_exposure_id
Do
$$
BEGIN
if not exists (select 0 from pg_catalog.pg_class where relname ='drug_exposure_id_seq')
then
CREATE SEQUENCE drug_exposure_id_seq INCREMENT BY 1 START WITH 1;
ALTER TABLE drug_exposure ALTER COLUMN drug_exposure_id SET DEFAULT nextval('drug_exposure_id_seq');
end if;
END
$$;

--Alter visit_detail table to automatically increment visit_detail_id
Do
$$
BEGIN
if not exists (select 0 from pg_catalog.pg_class where relname ='visit_detail_id_seq')
then
CREATE SEQUENCE IF NOT EXISTS visit_detail_id_seq INCREMENT BY 1 START WITH 1;
ALTER TABLE visit_detail ALTER COLUMN visit_detail_id SET DEFAULT nextval('visit_detail_id_seq');
end if;
END
$$;


--Alter device_exposure table to automatically increment device_exposure_id
Do
$$
BEGIN
if not exists (select 0 from pg_catalog.pg_class where relname ='device_exposure_id_seq')
then
CREATE SEQUENCE IF NOT EXISTS device_exposure_id_seq INCREMENT BY 1 START WITH 1;
ALTER TABLE device_exposure ALTER COLUMN device_exposure_id SET DEFAULT nextval('device_exposure_id_seq');
end if;
END
$$;
