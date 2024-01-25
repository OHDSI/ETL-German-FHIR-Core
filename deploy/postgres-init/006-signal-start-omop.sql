DO $$
BEGIN
    CREATE TABLE IF NOT EXISTS cds_cdm.load_status(
        write_status varchar(16) NULL
    );
    INSERT INTO cds_cdm.load_status (write_status) VALUES ('started');
END;
$$;