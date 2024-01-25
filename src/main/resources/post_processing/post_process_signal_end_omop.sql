DO $$
BEGIN
  -- Check if the table is empty
    INSERT INTO cds_cdm.load_status (write_status) VALUES ('finished');
    RAISE NOTICE "FHIR to OMOP Done!";
END;
$$;