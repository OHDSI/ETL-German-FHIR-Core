DO $$
BEGIN
  -- Check if the table is empty
    CREATE TABLE IF NOT EXISTS cds_cdm.load_status(
        write_status varchar(16) NULL
    );

    INSERT INTO cds_cdm.load_status (write_status) VALUES ('started');
    RAISE LOG "OMOP Vocab loaded - starting FHIR to OMOP conversion now!";
END;
$$;