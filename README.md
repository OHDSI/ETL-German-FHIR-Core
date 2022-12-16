# fhir-to-omop

This repository is currently dependent on the following repositories, for testing:

<https://gitlab.miracum.org/miracum/etl/deployment> (source fhir gateway)

<https://gitlab.miracum.org/miracum/etl/ohdsi-omop-v5> (test omop db)

[FHIR-TO-OMOP WIKI](https://gitlab.miracum.org/miracum/etl/fhir-to-omop/-/wikis/home)

## Run it

### Hardware Requirements

- RAM: 24 GB
- CPU: 12 vCPU
- HDD: 1 TB

Start the ETL process with docker-compose:

```bash
cd deploy
cp sample.env .env
# change the value of parameters
docker-compose up
```

## change the content in .env

### General information

1. This ETL job can read FHIR resources from a FHIR Server (Blaze or HAPI)
1. The using of __DATA_BEGINDATE__ and __DATA_ENDDATE__ on a FHIR Server is not yet possible -> Problem with Blaze FHIR Server :warning:
1. The begin and end date are corresponding to the column `last_updated_at` in FHIR-Gateway.
1. If __DATA_BEGINDATE__ and __DATA_ENDDATE__ are set with default date, the filter on date in FHIR-Gateway is deactivated.
1. If DATA_BEGINDATE=2021-01-01 and DATA_ENDDATE=2021-01-01, the job will import all the data between 2021-01-01 00:00:00 and 2021-01-01 23:59:59 into OMOP.
1. The initial load of OMOP CDM runs as Bulk load (Recommended). At the beginning of the job, all tables, which are referenced to __PERSON__ table will be emptied. The same applies to the tables in `cds_etl_helper` schema.
1. If FHIR-Gateway is reloaded with only new data, please run the Job as `incremental load` after the initial load, so that the tables in OMOP CDM will not be emptied at the beginning of the job.
1. If you want to load MedicationStatement resources to OMOP CDM in addition to MedicationAdministration, please set __APP_WRITEMEDICATIONSTATEMENT_ENABLED__ to _true_. The default value is _false_.

### Connect to your FHIR-Server
To connect to your local FHIR-Server you probably have to add an adjusted cacerts to the running container. Therefore you have to add the certifacte of your root-ca to a cacerts file and place it in the deploy folder. The mount path is already created in the docker-compose file. If you don't need the cacerts simply comment out the line.

### As BulkLoad

1. set the parameter __APP_BULKLOAD_ENABLED__ to _true_.
1. set __DATA_BEGINDATE__ and __DATA_ENDDATE__ to import the desired date from FHIR to OMOP.

For bulkload there is the possibility to search for referenced data (persons, visit_occurrences or visit_details) in RAM or in the OMOP DB.
The following parameter should be used for this purpose:

1. set the parameter __APP_DICTIONARYLOADINRAM_ENABLED__ to _true_ to search in RAM.
1. set the parameter __APP_DICTIONARYLOADINRAM_ENABLED__ to _false_ to search in OMOP DB.

For bulkload it is possible to run the job for different FHIR resources separately. All choosable steps are (case sensitive):

```bash
1. Observation
2. Procedure
3. Medication
4. MedicationAdministration
5. DepartmentCase
6. Condition
7. MedicationStatement
8. Immunization (GECCO dataset; still in pre-release)
9. DiagnosticReport (GECCO dataset; still in pre-release)
10. Consent (GECCO dataset; still in pre-release)
```

### As IncrementalLoad

1. set the parameter __APP_BULKLOAD_ENABLED__ to _false_.
1. set __DATA_BEGINDATE__ and __DATA_ENDDATE__ to import the desired data from FHIR to OMOP.

If you want to test this ETL process locally, use the `docker-compose.dev.yml`
to start the containers for OMOP and FHIR-Gateway pre-filled with sample data (see the [init-test-data](deploy/init-test-data) folder)

## Configuration

### sample.env

| Variables                  | Default Values                                                 | Comments                                         |
| -------------------------- | -------------------------------------------------------------- | ------------------------------------------------ |
| BATCH_CHUNKSIZE            | 5000                                                           | The size of the batch                            |
| BATCH_PAGINGSIZE           | 200000                                                         | The size of reading size                         |
| BATCH_THROTTLELIMIT        | 4                                                              | The number of threads used for this job          |
| LOGGING_LEVEL_ORG_MIRACUM  | INFO                                                           | Change to 'DEBUG' for more information in logging|
| DATA_FHIRGATEWAY_JDBCURL   | jdbc:postgresql://localhost:15432/fhir                         | The URL of FHIR gateway                          |
| DATA_FHIRGATEWAY_USERNAME  | postgres                                                       | The user name of FHIR gateway                    |
| DATA_FHIRGATEWAY_PASSWORD  | postgres                                                       | The password of FHIR gateway user                |
| DATA_FHIRGATEWAY_TABLENAME | resources                                                      | The name if the table containing FHIR resources  |
| DATA_FHIRSERVER_BASEURL    | _empty string_                                                 | Can be filled with a disired FHIR Base URL. Default is set to empty string |
| DATA_FHIRSERVER_USERNAME   | _empty string_                                                 | The user name of FHIR Server                     |
| DATA_FHIRSERVER_PASSWORD   | _empty string_                                                 | The password of FHIR Server user                 |
| DATA_FHIRSERVER_CONNECTIONTIMEOUT   | 3000                                                  | The connection timeout of FHIR Server            |
| DATA_FHIRSERVER_SOCKETTIMEOUT  | 3000                                                       | The socket timeout of FHIR Server                |
| DATA_OMOPCDM_JDBCURL       | jdbc:postgresql://localhost:5432/ohdsi                         | The JDBC URL of the OMOP DB                      |
| DATA_OMOPCDM_USERNAME      | ohdsi_admin_user                                               | The user name of OMOP                            |
| DATA_OMOPCDM_PASSWORD      | admin1                                                         | The password of OMOP user                        |
| DATA_OMOPCDM_SCHEMA        | cds_cdm                                                        | The schema name, where the data will be saved    |
| DATA_BEGINDATE             | 1800-01-01                                                     | It correspond to the 'last_updated_at' column in FHIR-Gateway|
| DATA_ENDDATE               | 2099-12-31                                                     | It correspond to the 'last_updated_at' column in FHIR-Gateway|
| PROMETHEUS_PUSHGATEWAY_ENABLED | true                                                       | Set to false to disable the prometheus pushgatway|
| PROMETHEUS_PUSHGATEWAY_URL | <https://localhost:9091>                                       | The URL of a Prometheus Pushgateway instance     |
| FHIR_SYSTEMS_DEPARTMENT    | <https://fhir.miracum.org/core/CodeSystem/fachabteilungen>     | The FHIR identifier system for departments       |
| FHIR_SYSTEMS_INTERPRETATION| <http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation> | The system URL for Interpretation          |
| APP_BULKLOAD_ENABLED       | false                                                          | Set to true to start bulk load                   |
| APP_DICTIONARYLOADINRAM_ENABLED| true                                                       | Set to false to search data in OMOP DB           |
| APP_WRITEMEDICATIONSTATEMENT_ENABLED | false                                                | Set to true to write MedicationStatement resources to OMOP |
| APP_STARTSINGLESTEP        | _emtpy string_                                                 | Set the desired step name to run the steps separately. Default is the initial load for all FHIR resources |
| SPRING_CACHE_CAFFEINE_SPEC_MAXIMUMSIZE    | 5000                                             | The size of the caffeineCache                   |

## International studies

### SQL script for mapping adjustments

This ETL process was developed in the context of a specific use case. Special mappings were necessary for this.
In order to use the data in OMOP for international studies, adjustments are required, e.g. to the type_concept_ids used.
By manually executing the SQL script __post_processing_international_adjustment__ (under the following path: fhir-to-omop\deploy\post_processing_international_adjustment.sql), the adjustments are performed directly on database level.
Please apply the SQL script only after successful completion of the ETL process FHIR-to-OMOP.

## Development

### Install Pre-commit hook to automatically check files on each commit

See <https://pre-commit.com> for more information

```sh
pre-commit install
pre-commit install --hook-type commit-msg
```
