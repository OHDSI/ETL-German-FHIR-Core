package org.miracum.etl.fhirtoomop;

import static org.miracum.etl.fhirtoomop.Constants.DEFAULT_BEGIN_DATE;
import static org.miracum.etl.fhirtoomop.Constants.DEFAULT_END_DATE;
import static org.miracum.etl.fhirtoomop.Constants.FETCH_RESOURCES_LOG;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONSENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DEPARTMENT_CASE;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DIAGNOSTIC_REPORT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_IMMUNIZATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_ADMINISTRATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_STATEMENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_PROCEDURE;
import static org.miracum.etl.fhirtoomop.Constants.STEP_ENCOUNTER_DEPARTMENT_KONTAKT;
import static org.miracum.etl.fhirtoomop.Constants.STEP_ENCOUNTER_INSTITUTION_KONTAKT;

import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.miracum.etl.fhirtoomop.listeners.ConditionStepListener;
import org.miracum.etl.fhirtoomop.listeners.ConsentStepListener;
import org.miracum.etl.fhirtoomop.listeners.DiagnosticReportStepListener;
import org.miracum.etl.fhirtoomop.listeners.EncounterDepartmentCaseStepListener;
import org.miracum.etl.fhirtoomop.listeners.EncounterMainStepListener;
import org.miracum.etl.fhirtoomop.listeners.FhirResourceProcessListener;
import org.miracum.etl.fhirtoomop.listeners.FhirToOmopJobListener;
import org.miracum.etl.fhirtoomop.listeners.ImmunizationStepListener;
import org.miracum.etl.fhirtoomop.listeners.MedicationAdministrationStepListener;
import org.miracum.etl.fhirtoomop.listeners.MedicationStatementStepListener;
import org.miracum.etl.fhirtoomop.listeners.MedicationStepListener;
import org.miracum.etl.fhirtoomop.listeners.ObservationStepListener;
import org.miracum.etl.fhirtoomop.listeners.PatientStepListener;
import org.miracum.etl.fhirtoomop.listeners.ProcedureStepListener;
import org.miracum.etl.fhirtoomop.mapper.ConditionMapper;
import org.miracum.etl.fhirtoomop.mapper.ConsentMapper;
import org.miracum.etl.fhirtoomop.mapper.DiagnosticReportMapper;
import org.miracum.etl.fhirtoomop.mapper.EncounterDepartmentCaseMapper;
import org.miracum.etl.fhirtoomop.mapper.EncounterInstitutionContactMapper;
import org.miracum.etl.fhirtoomop.mapper.ImmunizationMapper;
import org.miracum.etl.fhirtoomop.mapper.MedicationAdministrationMapper;
import org.miracum.etl.fhirtoomop.mapper.MedicationMapper;
import org.miracum.etl.fhirtoomop.mapper.MedicationStatementMapper;
import org.miracum.etl.fhirtoomop.mapper.ObservationMapper;
import org.miracum.etl.fhirtoomop.mapper.PatientMapper;
import org.miracum.etl.fhirtoomop.mapper.ProcedureMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.processor.ConditionProcessor;
import org.miracum.etl.fhirtoomop.processor.ConsentProcessor;
import org.miracum.etl.fhirtoomop.processor.DiagnosticReportProcessor;
import org.miracum.etl.fhirtoomop.processor.EncounterDepartmentCaseProcessor;
import org.miracum.etl.fhirtoomop.processor.EncounterInstitutionContactProcessor;
import org.miracum.etl.fhirtoomop.processor.ImmunizationStatusProcessor;
import org.miracum.etl.fhirtoomop.processor.MedicationAdministrationProcessor;
import org.miracum.etl.fhirtoomop.processor.MedicationProcessor;
import org.miracum.etl.fhirtoomop.processor.MedicationStatementProcessor;
import org.miracum.etl.fhirtoomop.processor.ObservationProcessor;
import org.miracum.etl.fhirtoomop.processor.PatientProcessor;
import org.miracum.etl.fhirtoomop.processor.ProcedureProcessor;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.task.configuration.DefaultTaskConfigurer;
import org.springframework.cloud.task.configuration.TaskConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The TaskConfiguration class contains the execution logic of the ETL process.
 *
 * @author Yuan Peng
 * @author Elisa Henke
 */
@Slf4j
@Configuration
@EnableBatchProcessing
public class TaskConfiguration {
  private static final InMemoryIncrementalIdMappings idMappings =
      new InMemoryIncrementalIdMappings();
  private final JobBuilderFactory jobBuilderFactory;
  private final StepBuilderFactory stepBuilderFactory;

  private static final DbMappings dbMappings = new DbMappings();
  private final OmopRepository repositories = new OmopRepository();

  @Value("${batch.chunkSize}")
  private int batchChunkSize;

  @Value("${batch.pagingSize}")
  private int pagingSize;

  @Value("${batch.throttleLimit}")
  private int throttleLimit;

  @Value("${app.version}")
  private String version;

  @Value("${app.bulkload.enabled}")
  private Boolean bulkload;

  @Value("${app.dictionaryLoadInRam.enabled}")
  private Boolean dictionaryLoadInRam;

  @Value("${data.beginDate}")
  private String beginDateStr;

  @Value("${data.endDate}")
  private String endDateStr;

  @Value("${app.startSingleStep}")
  private String startSingleStep;

  @Value("${app.writeMedicationStatement.enabled}")
  private Boolean writeMedicationStatement;

  @Value("${data.fhirGateway.tableName}")
  private String inputTableName;

  @Value("${data.omopCdm.schema}")
  private String outputSchemaName;

  @Value("${data.fhirServer.baseUrl}")
  private String fhirBaseUrl;

  @Bean
  public Boolean bulkload() {
    return this.bulkload;
  }

  /**
   * Constructor for objects of the class TaskConfiguration.
   *
   * @param jobBuilderFactory factory for the jobBuilder
   * @param stepBuilderFactory factory for the stepBuilder
   */
  @Autowired
  public TaskConfiguration(
      final JobBuilderFactory jobBuilderFactory, final StepBuilderFactory stepBuilderFactory) {

    this.jobBuilderFactory = jobBuilderFactory;
    this.stepBuilderFactory = stepBuilderFactory;
  }

  /**
   * Returns the internal id mappings
   *
   * @return the internal id mappings
   */
  @Bean
  public IIdMappings idMappings() {
    return idMappings;
  }

  /**
   * Returns the collections for the intermediate storage of data from OMOP CDM in RAM.
   *
   * @return the collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Bean
  public DbMappings dbMappings() {
    return dbMappings;
  }

  /**
   * Returns the OMOP CDM repositories.
   *
   * @return the OMOP CDM repositories
   */
  @Bean
  public OmopRepository repositories() {
    return repositories;
  }

  /**
   * Initialized a default task configurer
   *
   * @param defaultDataSource the data source to query against
   * @return a default task configurer
   */
  @Bean
  public TaskConfigurer taskConfigurer(DataSource defaultDataSource) {
    return new DefaultTaskConfigurer(defaultDataSource);
  }

  /** Add simpleMeterRegistry to the globalRegistry in Metrics. */
  @Bean
  public void setMetrics() {
    Metrics.globalRegistry.add(new SimpleMeterRegistry());
  }

  /**
   * Creates a partial SQL string for filtering the column last_updated_at in FHIR Gateway.
   *
   * @return partial SQL string for filtering the column last_updated_at in FHIR Gateway
   */
  private String setDateRange() {
    LocalDate begin = LocalDate.parse(beginDateStr);
    LocalDate end = LocalDate.parse(endDateStr);
    if (begin.isEqual(DEFAULT_BEGIN_DATE) && end.isEqual(DEFAULT_END_DATE)) {

      return "";
    }

    if (end.isBefore(LocalDate.now())) {

      return " AND last_updated_at BETWEEN '"
          + begin.atStartOfDay()
          + "' AND '"
          + end.atTime(23, 59, 59)
          + "'";
    }

    return " AND last_updated_at BETWEEN '"
        + begin.atStartOfDay()
        + "' AND '"
        + LocalDateTime.now()
        + "'";
  }

  /**
   * Creates a reader which reads FHIR resources from FHIR Gateway during bulk load.
   *
   * @param resourceType the FHIR resource type
   * @param dataSource the data source to query against
   * @return JdbcPagingItemReader for reading FHIR resources from FHIR Gateway
   */
  private JdbcPagingItemReader<FhirPsqlResource> createResourceReader(
      String resourceType, DataSource dataSource) {
    StringBuilder whereStatement = new StringBuilder();
    whereStatement.append("WHERE type = '" + resourceType + "'" + setDateRange());
    if (bulkload.equals(Boolean.TRUE)) {
      whereStatement.append("AND is_deleted = false");
    }

    return new JdbcPagingItemReaderBuilder<FhirPsqlResource>()
        .name("fhir-resource-reader")
        .selectClause("SELECT *")
        .fromClause("FROM " + inputTableName)
        .whereClause(whereStatement.toString())
        .sortKeys(getSortKeys("id"))
        .pageSize(pagingSize)
        .dataSource(dataSource)
        .rowMapper(BeanPropertyRowMapper.newInstance(FhirPsqlResource.class))
        .build();
  }

  /**
   * Creates a reader which only reads Encounter FHIR resources from FHIR Gateway during bulk load.
   *
   * @param dataSource the data source to query against
   * @param hasPartOf parameter to distinguish between department case and supply
   *     case/administrative case
   * @return JdbcPagingItemReader for reading FHIR resources from FHIR Gateway
   */
  private JdbcPagingItemReader<FhirPsqlResource> encounterReader(
      DataSource dataSource, String contactLevel) {
    var whereClause = new StringBuilder();
    whereClause.append("WHERE type = 'Encounter' AND ");
    if (bulkload.equals(Boolean.TRUE)) {
      whereClause.append("is_deleted = false AND ");
    }

    whereClause.append(
        "(data -> 'type' -> 0 -> 'coding' -> 0 ->> 'code'= '" + contactLevel + "') ");
    whereClause.append(setDateRange());

    return new JdbcPagingItemReaderBuilder<FhirPsqlResource>()
        .name("encounterReader")
        .selectClause("SELECT *")
        .fromClause("FROM " + inputTableName)
        .whereClause(whereClause.toString())
        .sortKeys(getSortKeys("id"))
        .pageSize(pagingSize)
        .dataSource(dataSource)
        .rowMapper(BeanPropertyRowMapper.newInstance(FhirPsqlResource.class))
        .build();
  }

  /**
   * Creates a reader which reads FHIR resources from FHIR Gateway during incremental load.
   *
   * @param dataSource the data source to query against
   * @param writeMedicationStatement parameter which indicates whether MedicationStatement resources
   *     are to be read in or not
   * @return JdbcPagingItemReader for reading FHIR resources from FHIR Gateway
   */
  private JdbcPagingItemReader<FhirPsqlResource> createIncrementalResourceReader(
      DataSource dataSource, Boolean writeMedicationStatement) {
    var whereClause = new StringBuilder();
    whereClause.append(
        "WHERE (type = 'Encounter' OR type='Patient' OR type='Medication' OR type='Observation' OR type='Procedure' OR type='Condition' OR type='MedicationAdministration'");
    if (writeMedicationStatement.booleanValue()) {
      whereClause.append("OR type='MedicationStatement'");
    }

    whereClause.append(")" + setDateRange());
    return new JdbcPagingItemReaderBuilder<FhirPsqlResource>()
        .name("fhir-incremental-resource-reader")
        .selectClause("SELECT * ")
        .fromClause("FROM " + inputTableName)
        .whereClause(whereClause.toString())
        .sortKeys(getSortKeys("id"))
        .pageSize(pagingSize)
        .dataSource(dataSource)
        .rowMapper(BeanPropertyRowMapper.newInstance(FhirPsqlResource.class))
        .build();
  }

  /**
   * Configures the direction of the sort in an ORDER BY clause.
   *
   * @param sortKey column name, which is used for sorting FHIR resources.
   * @return Map<String, Order> keys to sort by and the direction for each
   */
  private Map<String, Order> getSortKeys(String sortKey) {
    Map<String, Order> sortConfiguration = new HashMap<>();
    sortConfiguration.put(sortKey, Order.ASCENDING);
    return sortConfiguration;
  }

  /**
   * Creates a new SimpleAsyncTaskExecutor to use when executing the tasklet.
   *
   * @return SimpleAsyncTaskExecutor to use when executing the tasklet
   */
  @Bean
  public SimpleAsyncTaskExecutor taskExecutor() {
    return new SimpleAsyncTaskExecutor("fhir-to-omop");
  }

  /**
   * This job contains the processing logic depending on the decision whether the ETL process runs
   * as bulk load or incremental load.
   *
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @param singleStepFlow flow for bulk load with all FHIR resources or certain FHIR resource types
   * @param incrementalLoadFlow flow for incremental load
   * @return job configuration for bulk load or incremental load
   */
  @Bean
  public Job processFHIRData(
      @Qualifier("writerJdbcTemplate") final JdbcTemplate jdbcTemplate,
      Flow bulkloadFlow,
      Flow incrementalLoadFlow,
      Step postProcessStep,
      FhirToOmopJobListener fhirToOmopJobListener,
      JobExecutionDecider decider,
      @Qualifier("writerDataSource") DataSource outputDataSource) {
    if (!fhirToOmopJobListener.checkGernerallInput()) {
      return null;
    }
    return jobBuilderFactory
        .get("FHIR2OMOP")
        .listener(fhirToOmopJobListener)
        .start(initOmopDb(jdbcTemplate, outputDataSource))
        .next(decider)
        .on("BULKLOAD")
        .to(bulkloadFlow)
        .next(postProcessStep)
        .from(decider)
        .on("INCREMENTALLOAD")
        .to(incrementalLoadFlow)
        .next(postProcessStep)
        .end()
        .build();
  }

  /**
   * Creates a new CustomDecision decider.
   *
   * @return CustomDecision decider
   */
  @Bean
  public JobExecutionDecider decider() {
    return new CustomDecision();
  }

  /**
   * Creates a new SingleStepDecision decider.
   *
   * @return SingleStepDecision decider
   */
  @Bean
  public JobExecutionDecider singleStepDecider() {
    return new SingleStepDecision();
  }

  /**
   * Creates a new MedicationDecision decider.
   *
   * @return MedicationDecision decider
   */
  @Bean
  JobExecutionDecider medicationStepsDecider() {
    return new MedicationDecision();
  }

  /**
   * Defines the processing logic including the processing order for bulk load with all FHIR
   * resource types.
   *
   * @param patientProcessor processor which maps FHIR Patient resources to OMOP CDM
   * @param encounterProcessor processor which maps FHIR Encounter (administrative case/supply case
   *     resources to OMOP CDM
   * @param conditionProcessor processor which maps FHIR Condition resources to OMOP CDM
   * @param observationProcessor processor which maps FHIR Observation resources to OMOP CDM
   * @param procedureProcessor processor which maps FHIR Procedure resources to OMOP CDM
   * @param encounterSubProcessor processor which maps FHIR Encounter (department case) resources to
   *     OMOP CDM
   * @param medicationProcessor processor which maps FHIR Medication resources to OMOP CDM
   * @param medicationStepsFlow flow with processing logic for MedicationAdministration and
   *     MedicationStatement resources
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return processing logic for bulk load with all FHIR resource types
   */
  @Bean
  public Flow fullLoadFlow(
      Step stepProcessPatients,
      Step stepProcessEncounterInstitutionContact,
      Step stepProcessConditions,
      Step stepProcessObservations,
      Step stepProcessProcedures,
      Step stepEncounterDepartmentCase,
      Step stepProcessImmunization,
      Step stepProcessConsent,
      Step stepProcessDiagnosticReport,
      Flow medicationStepsFlow) {
    return new FlowBuilder<SimpleFlow>("bulkload")
        .start(stepProcessPatients)
        .next(stepProcessEncounterInstitutionContact)
            .next(stepEncounterDepartmentCase)
        .next(medicationStepsFlow)
        .next(stepProcessConditions)
        .next(stepProcessObservations)
        .next(stepProcessProcedures)
        .next(stepProcessImmunization)
        .next(stepProcessConsent)
        .next(stepProcessDiagnosticReport)
        .build();
  }

  /**
   * Defines the processing logic for MedicationAdministration and MedicationStatement resources
   * depending on the user's decision.
   *
   * @param medicationAdministrationProcessor processor which maps FHIR MedicationAdministration
   *     resources to OMOP CDM
   * @param medicationStatementProcessor processor which maps FHIR MedicationStatement to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return processing logic for MedicationAdministration and MedicationStatement resources
   */
  @Bean
  Flow medicationStepsFlow(
      Step stepProcessMedications,
      Step stepProcessMedicationAdministrations,
      Step stepProcessMedicationStatements) {
    return new FlowBuilder<SimpleFlow>("medicationSteps")
        .start(stepProcessMedications)
        .next(medicationStepsDecider())
        .on(FHIR_RESOURCE_MEDICATION_STATEMENT)
        .to(stepProcessMedicationStatements)
        .next(stepProcessMedicationAdministrations)
        .from(medicationStepsDecider())
        .on("SKIPPED")
        .to(stepProcessMedicationAdministrations)
        .build();
  }

  /**
   * Defines the processing logic for bulk load with the possible selection to write only specific
   * FHIR resource types.
   *
   * @param bulkloadFlow flow with processing logic for bulk load with all FHIR resource types
   * @param observationProcessor processor which maps FHIR Observation resources to OMOP CDM
   * @param conditionProcessor processor which maps FHIR Condition resources to OMOP CDM
   * @param procedureProcessor processor which maps FHIR Procedure resources to OMOP CDM
   * @param medicationAdministrationProcessor processor which maps FHIR MedicationAdministration
   *     resources to OMOP CDM
   * @param medicationStatementProcessor processor which maps FHIR MedicationStatement to OMOP CDM
   * @param medicationProcessor processor which maps FHIR Medication to OMOP CDM
   * @param encounterSubProcessor processor which maps FHIR Encounter (department case) resources to
   *     OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return processing logic for bulk load with the possible selection to write only specific FHIR
   *     resource types
   */
  @Bean
  public Flow bulkloadFlow(
      Flow fullLoadFlow,
      Step stepProcessObservations,
      Step stepProcessConditions,
      Step stepProcessProcedures,
      Step stepProcessMedicationAdministrations,
      Step stepProcessMedicationStatements,
      Step stepProcessMedications,
      Step stepEncounterDepartmentCase,
      Step stepProcessImmunization,
      Step stepProcessConsent,
      Step stepProcessDiagnosticReport) {
    return new FlowBuilder<SimpleFlow>("singleStep")
        .start(singleStepDecider())
        .on("All")
        .to(fullLoadFlow)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_OBSERVATION)
        .to(stepProcessObservations)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_CONDITION)
        .to(stepProcessConditions)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_PROCEDURE)
        .to(stepProcessProcedures)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_MEDICATION_ADMINISTRATION)
        .to(stepProcessMedicationAdministrations)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_MEDICATION_STATEMENT)
        .to(stepProcessMedicationStatements)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_DEPARTMENT_CASE)
        .to(stepEncounterDepartmentCase)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_IMMUNIZATION)
        .to(stepProcessImmunization)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_CONSENT)
        .to(stepProcessConsent)
        .from(singleStepDecider())
        .on(FHIR_RESOURCE_DIAGNOSTIC_REPORT)
        .to(stepProcessDiagnosticReport)
        .build();
  }

  /**
   * Defines the processing logic including the processing order for incremental load with all FHIR
   * resource types.
   *
   * @param stepProcessPatients Step which transforms FHIR Patient resources to OMOP CDM
   * @param stepProcessEncounterInstitutionContact processor which transforms FHIR Encounter
   *     (administrative case/supply case resources to OMOP CDM
   * @param stepProcessConditions Step which transforms FHIR Condition resources to OMOP CDM
   * @param stepProcessObservations Step which transforms FHIR Observation resources to OMOP CDM
   * @param stepProcessProcedures Step which transforms FHIR Procedure resources to OMOP CDM
   * @param stepEncounterDepartmentCase Step which transforms FHIR Encounter (department case)
   *     resources to OMOP CDM
   * @param medicationStepsFlow Flow which process FHIR Medication, MedicationAdministration and
   *     MedicationStatement resources to OMOP CDM
   * @return processing logic for incremental load with all FHIR resource types
   */
  @Bean
  public Flow incrementalLoadFlow(
      Step stepProcessPatients,
      Step stepProcessEncounterInstitutionContact,
      Step stepProcessConditions,
      Step stepProcessObservations,
      Step stepProcessProcedures,
      Step stepEncounterDepartmentCase,
      Step stepProcessImmunization,
      Step stepProcessConsent,
      Step stepProcessDiagnosticReport,
      Flow medicationStepsFlow) {
    return new FlowBuilder<SimpleFlow>("incrementalLoad")
        .start(stepProcessPatients)
        .next(stepProcessEncounterInstitutionContact)
        .next(stepEncounterDepartmentCase)
        .next(medicationStepsFlow)
        .next(stepProcessConditions)
        .next(stepProcessObservations)
        .next(stepProcessProcedures)
        .next(stepProcessImmunization)
        .next(stepProcessConsent)
        .next(stepProcessDiagnosticReport)
        .build();
  }

  /**
   * Defines the step to initialize OMOP CDM. This includes creating new tables in OMOP CDM or
   * changing existing table definitions.
   *
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return step which initializes OMOP CDM
   */
  @Bean
  public Step initOmopDb(JdbcTemplate jdbcTemplate, DataSource outputDataSource) {
    return stepBuilderFactory
        .get("initJobInfo")
        .tasklet(new InitOmopDb(jdbcTemplate, version, bulkload, outputDataSource, repositories))
        .build();
  }

  /**
   * Defines the reader which reads FHIR resources from FHIR Gateway during incremental load.
   *
   * @param dataSource the data source to query against
   * @return reader for incremental load
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readFhirResources(
      @Qualifier("readerDataSource") final DataSource dataSource) {

    return createIncrementalResourceReader(dataSource, writeMedicationStatement);
  }

  /**
   * Defines the reader for FHIR Patient resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Patient resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlPatient(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {

    var resourceType = "Patient";
    log.info(FETCH_RESOURCES_LOG, resourceType);
    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.PATIENT.getDisplay(), "");
  }

  /**
   * Create FHIR Server REST API paging reader.
   *
   * @param client FHIR Server Client
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param resourceTypeName Enumeration name of FHIR resource
   * @return a new FHIR Server REST API paging reader.
   */
  private FhirServerItemReader fhirServerItemReader(
      IGenericClient client, IParser parser, String resourceTypeName, String stepName) {
    FhirServerItemReader fhirServerItemReader = new FhirServerItemReader();
    fhirServerItemReader.setFhirClient(client);
    fhirServerItemReader.setPageSize(pagingSize);
    fhirServerItemReader.setResourceTypeClass(resourceTypeName);
    fhirServerItemReader.setBeginDate(beginDateStr);
    fhirServerItemReader.setEndDate(endDateStr);
    fhirServerItemReader.setFhirParser(parser);
    fhirServerItemReader.setStepName(stepName);
    return fhirServerItemReader;
  }

  /**
   * Defines the step for processing FHIR Patient resources. This step loads and processes Patient
   * resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param patientProcessor processor which maps FHIR Patient resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @return step for processing FHIR Patient resources
   */
  @Bean
  public Step stepProcessPatients(
      PatientProcessor patientProcessor,
      PatientStepListener listener,
      ItemStreamReader<FhirPsqlResource> readerPsqlPatient,
      ItemWriter<OmopModelWrapper> writer) {

    var stepProcessPatientsBuilder =
        stepBuilderFactory
            .get("stepProcessPatients")
            .listener(listener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlPatient)
            .processor(patientProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);
    if (StringUtils.isBlank(fhirBaseUrl)) {

      stepProcessPatientsBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }
    return stepProcessPatientsBuilder.build();
  }

  /**
   * Defines the processor for FHIR Patient resources. The PatientProcessor contains the business
   * logic to map FHIR Patient resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param idMappings reference to internal id mappings
   * @return processor for FHIR Patient resources
   */
  @Bean
  public PatientProcessor patientProcessor(IParser parser, PatientMapper patientMapper) {

    return new PatientProcessor(patientMapper, parser);
  }

  /**
   * Defines the reader for FHIR Encounter (administrative case/supply case) resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Encounter (administrative case/supply case) resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> encounterMainReader(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Encounter";
    log.info(FETCH_RESOURCES_LOG, resourceType);
    if (StringUtils.isBlank(fhirBaseUrl)) {
      return encounterReader(dataSource, "einrichtungskontakt");
    }
    return fhirServerItemReader(
        client,
        fhirParser,
        ResourceType.ENCOUNTER.getDisplay(),
        STEP_ENCOUNTER_INSTITUTION_KONTAKT);
  }

  /**
   * Defines the step for processing FHIR Encounter (administrative case/supply case) resources.
   * This step loads and processes Encounter (administrative case/supply case) resources from FHIR
   * Gateway and writes them to OMOP CDM.
   *
   * @param encounterProcessor processor which maps FHIR Encounter (administrative case/supply case)
   *     resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @return step for processing FHIR Encounter (administrative case/supply case) resources
   */
  @Bean
  public Step stepProcessEncounterInstitutionContact(
      EncounterInstitutionContactProcessor encounterProcessor,
      EncounterMainStepListener encounterMainStepListener,
      ItemStreamReader<FhirPsqlResource> encounterMainReader,
      ItemWriter<OmopModelWrapper> writer) {
    var encounterMainStepBuilder =
        stepBuilderFactory
            .get("stepProcessEncounterInstitutionContact")
            .listener(encounterMainStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(encounterMainReader)
            .processor(encounterProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      encounterMainStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return encounterMainStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR Encounter (administrative case/supply case) resources. The
   * EncounterInstitutionContactProcessor contains the business logic to map FHIR Encounter
   * (administrative case/supply case) resources to OMOP CDM.
   *
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param idMappings reference to internal id mappings
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @return processor for FHIR Encounter (administrative case/supply case) resources
   */
  @Bean
  public EncounterInstitutionContactProcessor encounterProcessor(
      IParser parser, EncounterInstitutionContactMapper encounterMainMapper) {

    return new EncounterInstitutionContactProcessor(encounterMainMapper, parser);
  }

  /**
   * Defines the reader for FHIR Encounter (department case) resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Encounter (department case) resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> encounterSubReader(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    log.info(FETCH_RESOURCES_LOG, "Department Cases");

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return encounterReader(dataSource, "abteilungskontakt");
    }
    return fhirServerItemReader(
        client, fhirParser, ResourceType.ENCOUNTER.getDisplay(), STEP_ENCOUNTER_DEPARTMENT_KONTAKT);
  }

  /**
   * Defines the step for processing FHIR Encounter (department case) resources. This step loads and
   * processes Encounter (department case) resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param encounterSubProcessor processor which maps FHIR Encounter (department case) resources to
   *     OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @return step for processing FHIR Encounter (department case) resources
   */
  @Bean
  public Step stepEncounterDepartmentCase(
      EncounterDepartmentCaseProcessor encounterDepartmentCaseProcessor,
      ItemStreamReader<FhirPsqlResource> encounterSubReader,
      ItemWriter<OmopModelWrapper> writer,
      EncounterDepartmentCaseStepListener encounterDepartmentCaseStepListener) {
    var encounterSubStepBuilder =
        stepBuilderFactory
            .get("stepProcessEncounterDepartmentCase")
            .listener(encounterDepartmentCaseStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(encounterSubReader)
            .processor(encounterDepartmentCaseProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      encounterSubStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return encounterSubStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR Encounter (department case) resources. The
   * EncounterDepartmentCaseProcessor contains the business logic to map FHIR Encounter (department
   * case) resources to OMOP CDM.
   *
   * @param fhirSystems reference to naming and coding systems used in FHIR resources.
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param idMappings reference to internal id mappings
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @return processor for FHIR Encounter (department case) resources
   */
  @Bean
  public EncounterDepartmentCaseProcessor encounterSubProcessor(
      IParser parser, EncounterDepartmentCaseMapper encounterSubMapper) {
    return new EncounterDepartmentCaseProcessor(encounterSubMapper, parser);
  }

  /**
   * Defines the reader for FHIR Condition resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Condition resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlConditions(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Condition";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.CONDITION.getDisplay(), "");
  }

  /**
   * Defines the step for processing FHIR Condition resources. This step loads and processes
   * Condition resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param conditionProcessor processor which maps FHIR Condition resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return step for processing FHIR Condition resources
   */
  @Bean
  public Step stepProcessConditions(
      ConditionProcessor conditionProcessor,
      ItemWriter<OmopModelWrapper> writer,
      ConditionStepListener conditionStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlConditions,
      @Qualifier("writerDataSource") final DataSource dataSource) {

    var conditionStepBuilder =
        stepBuilderFactory
            .get("stepProcessConditions")
            .listener(conditionStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlConditions)
            .processor(conditionProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      conditionStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return conditionStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR Condition resources. The ConditionProcessor contains the
   * business logic to map FHIR Condition resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param idMappings reference to internal id mappings
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @return processor for FHIR Condition resources
   */
  @Bean
  public ConditionProcessor conditionProcessor(IParser parser, ConditionMapper conditionMapper) {
    return new ConditionProcessor(conditionMapper, parser);
  }

  /**
   * Defines the reader for FHIR Observation resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Observation resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlObservations(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Observation";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.OBSERVATION.getDisplay(), "");
  }

  /**
   * Defines the step for processing FHIR Observation resources. This step loads and processes
   * Observation resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param observationProcessor processor which maps FHIR Observation resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return step for processing FHIR Observation resources
   */
  @Bean
  public Step stepProcessObservations(
      ObservationProcessor observationProcessor,
      ItemWriter<OmopModelWrapper> writer,
      ObservationStepListener observationStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlObservations,
      @Qualifier("writerDataSource") final DataSource dataSource) {

    var observationStepBuilder =
        stepBuilderFactory
            .get("stepProcessObservations")
            .listener(observationStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlObservations)
            .processor(observationProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      observationStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return observationStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR Observation resources. The ObservationProcessor contains the
   * business logic to map FHIR Observation resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param idMappings reference to internal id mappings
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @return processor for FHIR Observation resources
   */
  @Bean
  public ObservationProcessor observationProcessor(
      IParser parser, ObservationMapper observationMapper) {
    return new ObservationProcessor(observationMapper, parser);
  }

  /**
   * Defines the reader for FHIR Procedure resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Procedure resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlProcedures(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Procedure";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.PROCEDURE.getDisplay(), "");
  }

  /**
   * Defines the step for FHIR Procedure resources. This step loads and processes Procedure
   * resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param procedureProcessor processor which maps FHIR Procedure resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return step for FHIR Procedure resources
   */
  @Bean
  public Step stepProcessProcedures(
      ProcedureProcessor procedureProcessor,
      ItemWriter<OmopModelWrapper> writer,
      ProcedureStepListener procedureStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlProcedures,
      @Qualifier("writerDataSource") final DataSource dataSource) {

    var procedureStepBuilder =
        stepBuilderFactory
            .get("stepProcessProcedures")
            .listener(procedureStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlProcedures)
            .processor(procedureProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      procedureStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return procedureStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR Procedure resources. The ProcedureProcessor contains the
   * business logic to map FHIR Procedure resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param idMappings reference to internal id mappings.
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @return processor for FHIR Procedure resources
   */
  @Bean
  public ProcedureProcessor procedureProcessor(IParser parser, ProcedureMapper procedureMapper) {

    return new ProcedureProcessor(procedureMapper, parser);
  }

  /**
   * Defines the reader for FHIR Medication resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR Medication resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlMedications(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Medication";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.MEDICATION.getDisplay(), "");
  }

  /**
   * Defines the step for FHIR Medication resources. This step loads and processes Medication
   * resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param medicationProcessor processor which maps FHIR Medication resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @return step for FHIR Medication resources
   */
  @Bean
  public Step stepProcessMedications(
      MedicationProcessor medicationProcessor,
      ItemWriter<OmopModelWrapper> writer,
      MedicationStepListener medicationStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlMedications,
      @Qualifier("writerDataSource") final DataSource dataSource) {

    var medicationStepBuilder =
        stepBuilderFactory
            .get("stepProcessMedications")
            .listener(medicationStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlMedications)
            .processor(medicationProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      medicationStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return medicationStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR Medication resources. The MedicationProcessor contains the
   * business logic to map FHIR Medication resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param idMappings reference to internal id mappings
   * @return processor for FHIR Medication resources
   */
  @Bean
  public MedicationProcessor medicationProcessor(
      IParser parser, MedicationMapper medicationMapper) {

    return new MedicationProcessor(medicationMapper, parser);
  }

  /**
   * Defines the reader for FHIR MedicationAdministration resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR MedicationAdministration resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlMedicationAdministrations(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "MedicationAdministration";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(
        client, fhirParser, ResourceType.MEDICATIONADMINISTRATION.getDisplay(), "");
  }

  /**
   * Defines the step for FHIR MedicationAdministration resources. This step loads and processes
   * MedicationAdministration resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param medicationAdministrationProcessor processor which maps FHIR MedicationAdministration
   *     resources to OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return step for FHIR MedicationAdministration resources
   */
  @Bean
  public Step stepProcessMedicationAdministrations(
      MedicationAdministrationProcessor medicationAdministrationProcessor,
      ItemWriter<OmopModelWrapper> writer,
      MedicationAdministrationStepListener medicationAdministrationStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlMedicationAdministrations,
      @Qualifier("writerDataSource") final DataSource dataSource) {

    var medicationAdministrationStepBuilder =
        stepBuilderFactory
            .get("stepProcessMedicationAdministrations")
            .listener(medicationAdministrationStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlMedicationAdministrations)
            .processor(medicationAdministrationProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      medicationAdministrationStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }

    return medicationAdministrationStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR MedicationAdministration resources. The
   * MedicationAdministrationProcessor contains the business logic to map FHIR
   * MedicationAdministration resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param idMappings reference to internal id mappings.
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @return processor for FHIR MedicationAdministration resources
   */
  @Bean
  public MedicationAdministrationProcessor medicationAdministrationProcessor(
      IParser parser, MedicationAdministrationMapper medicationAdministrationMapper) {

    return new MedicationAdministrationProcessor(medicationAdministrationMapper, parser);
  }

  /**
   * Defines the reader for FHIR MedicationStatement resources.
   *
   * @param dataSource the data source to query against
   * @return reader for FHIR MedicationStatement resources
   */
  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlMedicationStatements(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "MedicationStatement";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(
        client, fhirParser, ResourceType.MEDICATIONSTATEMENT.getDisplay(), "");
  }

  /**
   * Defines the step for FHIR MedicationStatement resources. This step loads and processes
   * MedicationStatement resources from FHIR Gateway and writes them to OMOP CDM.
   *
   * @param medicationStatementProcessor processor which maps FHIR MedicationStatement resources to
   *     OMOP CDM
   * @param writer the writer which writes the data to OMOP CDM
   * @param jdbcTemplate JdbcTemplate for the execution of SQL statements
   * @return step for FHIR MedicationStatement resources
   */
  @Bean
  public Step stepProcessMedicationStatements(
      MedicationStatementProcessor medicationStatementProcessor,
      MedicationStatementStepListener medicationStatementStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlMedicationStatements,
      ItemWriter<OmopModelWrapper> writer) {

    var medicationStatementStepBuilder =
        stepBuilderFactory
            .get("stepProcessMedicationStatements")
            .listener(medicationStatementStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlMedicationStatements)
            .processor(medicationStatementProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      medicationStatementStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }
    return medicationStatementStepBuilder.build();
  }

  /**
   * Defines the processor for FHIR MedicationStatement resources. The MedicationStatementProcessor
   * contains the business logic to map FHIR MedicationStatement resources to OMOP CDM.
   *
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   * @param idMappings reference to internal id mappings
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references.
   * @return processor for FHIR MedicationStatement resources
   */
  @Bean
  public MedicationStatementProcessor medicationStatementProcessor(
      IParser parser, MedicationStatementMapper medicationStatementMapper) {

    return new MedicationStatementProcessor(medicationStatementMapper, parser);
  }

  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlImmunization(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Immunization";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.IMMUNIZATION.getDisplay(), "");
  }

  @Bean
  public Step stepProcessImmunization(
      ImmunizationStatusProcessor immunizationStatusProcessor,
      ImmunizationStepListener immunizationStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlImmunization,
      ItemWriter<OmopModelWrapper> writer) {

    var immunizationStepBuilder =
        stepBuilderFactory
            .get("stepProcessImmunizations")
            .listener(immunizationStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlImmunization)
            .processor(immunizationStatusProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      immunizationStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }
    return immunizationStepBuilder.build();
  }

  @Bean
  public ImmunizationStatusProcessor immunizationProcessor(
      IParser parser, ImmunizationMapper immunizationStatusMapper) {

    return new ImmunizationStatusProcessor(immunizationStatusMapper, parser);
  }

  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlConsent(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "Consent";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.CONSENT.getDisplay(), "");
  }

  @Bean
  public Step stepProcessConsent(
      ConsentProcessor consentProcessor,
      ConsentStepListener consentStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlConsent,
      ItemWriter<OmopModelWrapper> writer) {

    var consentStepBuilder =
        stepBuilderFactory
            .get("stepProcessConsent")
            .listener(consentStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlConsent)
            .processor(consentProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);
    if (bulkload.equals(Boolean.TRUE)) {
      consentStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }
    return consentStepBuilder.build();
  }

  @Bean
  public ConsentProcessor consentProcessor(IParser parser, ConsentMapper consentMapper) {

    return new ConsentProcessor(consentMapper, parser);
  }

  @Bean
  @StepScope
  public ItemStreamReader<FhirPsqlResource> readerPsqlDiagnosticReport(
      @Qualifier("readerDataSource") final DataSource dataSource,
      IGenericClient client,
      IParser fhirParser) {
    var resourceType = "DiagnosticReport";
    log.info(FETCH_RESOURCES_LOG, resourceType);

    if (StringUtils.isBlank(fhirBaseUrl)) {
      return createResourceReader(resourceType, dataSource);
    }
    return fhirServerItemReader(client, fhirParser, ResourceType.DIAGNOSTICREPORT.getDisplay(), "");
  }

  @Bean
  public Step stepProcessDiagnosticReport(
      DiagnosticReportProcessor diagnosticReportProcessor,
      DiagnosticReportStepListener diagnosticReportStepListener,
      ItemStreamReader<FhirPsqlResource> readerPsqlDiagnosticReport,
      ItemWriter<OmopModelWrapper> writer) {

    var diagnosticReportStepBuilder =
        stepBuilderFactory
            .get("stepProcessDiagnosticReport")
            .listener(diagnosticReportStepListener)
            .<FhirPsqlResource, OmopModelWrapper>chunk(batchChunkSize)
            .reader(readerPsqlDiagnosticReport)
            .processor(diagnosticReportProcessor)
            .listener(new FhirResourceProcessListener())
            .writer(writer);

    if (bulkload.equals(Boolean.TRUE)) {
      diagnosticReportStepBuilder.throttleLimit(throttleLimit).taskExecutor(taskExecutor());
    }
    return diagnosticReportStepBuilder.build();
  }

  @Bean
  public DiagnosticReportProcessor diagnosticReportProcessor(
      IParser parser, DiagnosticReportMapper diagnosticReportMapper) {

    return new DiagnosticReportProcessor(diagnosticReportMapper, parser);
  }

  /**
   * Defines the task for post processing.
   *
   * @param dataSource the data source to query against
   * @return task for post processing
   */
  @Bean
  public PostProcessTask postProsessTask(DataSource dataSource) {
    return new PostProcessTask(dataSource, startSingleStep, bulkload);
  }

  /**
   * Defines the step for post processing. This step executes SQL scripts for complex mappings at
   * database level.
   *
   * @param dataSource the data source to query against
   * @return step for post processing
   */
  @Bean
  public Step postProcessStep(@Qualifier("writerDataSource") final DataSource dataSource) {
    return stepBuilderFactory.get("stepPostProcess").tasklet(postProsessTask(dataSource)).build();
  }
}
