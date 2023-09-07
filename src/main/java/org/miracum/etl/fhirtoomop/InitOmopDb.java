package org.miracum.etl.fhirtoomop;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.miracum.etl.fhirtoomop.model.OrphaSnomedMapping;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.miracum.etl.fhirtoomop.utils.ExecuteSqlScripts;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The InitOmopDb class is used to initialize OMOP CDM. This includes creating new tables in OMOP
 * CDM or changing existing table definitions.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class InitOmopDb implements Tasklet {

  private final JdbcTemplate jdbcTemplate;
  private final String jobVersion;
  private final Boolean bulkload;
  private final DataSource outputDataSource;
  private final OmopRepository omopRepository;

  /**
   * Constructor for objects of the class InitOmopDb.
   *
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @param jobVersion the version of the ETL job
   * @param bulkload flag to differentiate between bulk load or incremental load
   * @param outputDataSource the data source to query against
   * @param omopRepository OMOP CDM repositories
   */
  public InitOmopDb(
      JdbcTemplate jdbcTemplate,
      String jobVersion,
      Boolean bulkload,
      DataSource outputDataSource,
      OmopRepository omopRepository) {
    this.jdbcTemplate = jdbcTemplate;
    this.jobVersion = jobVersion;
    this.bulkload = bulkload;
    this.outputDataSource = outputDataSource;
    this.omopRepository = omopRepository;
  }

  /**
   * Executes the tasks to initialize OMOP CDM
   *
   * @param contribution buffers changes until they can be applied to a chunk boundary
   * @param chunkContext context object for weakly typed data stored for the duration of a chunk
   * @return the processing status
   */
  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
      throws Exception {
    importToCdmSourceTable();
    insertOrphaSnomedMapping();
    modifyingTable(contribution);
    migrationFhirIdToOmopIdTable();
    return RepeatStatus.FINISHED;
  }

  /**
   * Modifies existing tables in OMOP CDM by adding indices adding or renaming columns as well as
   * creates materialized views in cds_etl_helper schema.
   *
   * @param contribution contribution buffers changes until they can be applied to a chunk boundary
   */
  private void modifyingTable(StepContribution contribution) throws SQLException, IOException {
    ExecuteSqlScripts executeSqlScripts = new ExecuteSqlScripts(outputDataSource, contribution);
    Resource addColumns = new ClassPathResource("pre_processing/pre_process_alter_tables.sql");
    Resource addIndex = new ClassPathResource("pre_processing/pre_process_add_index.sql");
    Resource alterMedicationIdMap =
        new ClassPathResource("pre_processing/pre_process_alter_medication_id_map.sql");
    Resource createEtlHelperTables =
        new ClassPathResource("pre_processing/pre_process_create_etl_helper_tables.sql");

    executeSqlScripts.executeSQLScript(createEtlHelperTables);
    executeSqlScripts.executeSQLScript(addColumns);
    executeSqlScripts.executeSQLScript(addIndex);
    executeSqlScripts.executeSQLScript(alterMedicationIdMap);
  }

  /**
   * Migrates the fhir_id_to_omop_id_map table inclusive the contained data from the schema cds_cdm
   * to cds_etl_helper.
   *
   * <p>This method is deprecated since version v1.16.1
   */
  @Deprecated
  private void migrationFhirIdToOmopIdTable() {
    if (bulkload.equals(Boolean.TRUE)) {
      return;
    }
    var ifTableInCdsSchema = "select to_regclass('cds_cdm.fhir_id_to_omop_id_map');";
    var ifTableInHelperSchema = "select to_regclass('cds_etl_helper.fhir_id_to_omop_id_map');";
    var getRowCount = "select count(*) from ";
    var cdsSchema = jdbcTemplate.queryForMap(ifTableInCdsSchema).get("to_regclass");
    var helperSchema = jdbcTemplate.queryForMap(ifTableInHelperSchema).get("to_regclass");

    if (cdsSchema != null && helperSchema != null) {
      var cdsSchemaRowCount =
          Integer.parseInt(
              jdbcTemplate
                  .queryForMap(getRowCount + "cds_cdm.fhir_id_to_omop_id_map")
                  .get("count")
                  .toString());
      var helperSchemaRowCount =
          Integer.parseInt(
              jdbcTemplate
                  .queryForMap(getRowCount + "cds_etl_helper.fhir_id_to_omop_id_map")
                  .get("count")
                  .toString());
      if (cdsSchemaRowCount != 0 && helperSchemaRowCount == 0) {
        jdbcTemplate.execute(
            "INSERT INTO cds_etl_helper.fhir_id_to_omop_id_map (select * from cds_cdm.fhir_id_to_omop_id_map)");
        jdbcTemplate.execute(
            "select setval('cds_etl_helper.fhir_id_to_omop_id_map_fhir_omop_id_seq',(select max(fhir_omop_id) from cds_etl_helper.fhir_id_to_omop_id_map)) ");
      }
    }
  }

  /** Creates and fills the orpha_snomed_mapping table in OMOP CDM with data from a csv file. */
  private void insertOrphaSnomedMapping() {
    createOrphaSnomedMapping();
    log.info("Inserting data into orpha_snomed_mapping.");
    Resource orphaSnomed = new ClassPathResource("orpha_snomed_mapping.csv");
    try {
      omopRepository.getOrphaSnomedMappingRepository().truncateTable();
    } catch (Exception e) {
      log.error("No file found in {}", orphaSnomed);
    }
    var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    try (BufferedReader br =
        new BufferedReader(new FileReader(orphaSnomed.getFile(), StandardCharsets.UTF_8))) {
      br.readLine();
      var line = "";
      List<OrphaSnomedMapping> orphaSnomedMappings = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        var splitedLine = Arrays.asList(line.split(";"));
        orphaSnomedMappings.add(
            OrphaSnomedMapping.builder()
                .orphaCode(splitedLine.get(0).toString())
                .orphaConceptId(Integer.parseInt(splitedLine.get(1)))
                .snomedCode(splitedLine.get(2).toString())
                .snomedConceptId(Integer.parseInt(splitedLine.get(3)))
                .snomedDomainId(splitedLine.get(4))
                .snomedDomainConceptId(Integer.parseInt(splitedLine.get(5)))
                .orphaValidStartDate(LocalDate.parse(splitedLine.get(6), formatter))
                .orphaValidEndDate(LocalDate.parse(splitedLine.get(7), formatter))
                .mappingValidStartDate(LocalDate.parse(splitedLine.get(8), formatter))
                .mappingValidEndDate(LocalDate.parse(splitedLine.get(9), formatter))
                .build());
      }

      omopRepository.getOrphaSnomedMappingRepository().saveAll(orphaSnomedMappings);
      omopRepository.getOrphaSnomedMappingRepository().flush();
      log.info("Inserted {} rows into orpha_snomed_mapping.", orphaSnomedMappings.size());
    } catch (FileNotFoundException e) {
      log.error(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /** Creates orpha_snomed_mapping table in cds_etl_helper schema in OMOP. */
  private void createOrphaSnomedMapping() {
    String createIdTable =
        "CREATE TABLE IF NOT EXISTS cds_etl_helper.orpha_snomed_mapping (orpha_code varchar(255) NOT NULL, "
            + "orpha_concept_id INTEGER, snomed_code varchar(255), snomed_concept_id INTEGER, "
            + "snomed_domain_id varchar(64), snomed_domain_concept_id INTEGER, "
            + "orpha_valid_start_date date, orpha_valid_end_date date, "
            + "mapping_valid_start_date date, mapping_valid_end_date date); "
            + "CREATE INDEX IF NOT EXISTS idx_orpha_code ON cds_etl_helper.orpha_snomed_mapping (orpha_code);"
            + "CREATE INDEX IF NOT EXISTS idx_orpha_concept ON cds_etl_helper.orpha_snomed_mapping (orpha_concept_id);"
            + "CREATE INDEX IF NOT EXISTS idx_snomed_code ON cds_etl_helper.orpha_snomed_mapping (snomed_code);"
            + "CREATE INDEX IF NOT EXISTS idx_snomed_concept ON cds_etl_helper.orpha_snomed_mapping (snomed_concept_id);"
            + "CREATE INDEX IF NOT EXISTS idx_snomed_domain ON cds_etl_helper.orpha_snomed_mapping (snomed_domain_id);"
            + "CREATE INDEX IF NOT EXISTS idx_snomed_domain_concept ON cds_etl_helper.orpha_snomed_mapping (snomed_domain_concept_id);"
            + "CREATE INDEX IF NOT EXISTS idx_orpha_start ON cds_etl_helper.orpha_snomed_mapping (orpha_valid_start_date);"
            + "CREATE INDEX IF NOT EXISTS idx_orpha_end ON cds_etl_helper.orpha_snomed_mapping (orpha_valid_end_date);"
            + "CREATE INDEX IF NOT EXISTS idx_mapping_start ON cds_etl_helper.orpha_snomed_mapping (mapping_valid_start_date);"
            + "CREATE INDEX IF NOT EXISTS idx_mapping_end ON cds_etl_helper.orpha_snomed_mapping (mapping_valid_end_date);"
            + "COMMIT;";
    jdbcTemplate.execute(createIdTable);
  }

  /** Updates the cdm_source table in OMOP CDM. */
  private void importToCdmSourceTable() {
    var importStr =
        "INSERT INTO cdm_source (cdm_source_name,cdm_etl_reference,source_release_date,cdm_version,vocabulary_version)VALUES(?,?,?,?,?);";
    jdbcTemplate.update(
        importStr,
        "FHIR-to-OMOP " + LocalDateTime.now().toString(),
        jobVersion,
        LocalDate.now(),
        "v5.3.1",
        "v5.0");
  }
}
