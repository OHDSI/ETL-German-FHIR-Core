package org.miracum.etl.fhirtoomop.listeners;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONSENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DEPARTMENT_CASE;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DIAGNOSTIC_REPORT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_IMMUNIZATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_ADMINISTRATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_STATEMENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_PROCEDURE;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.miracum.etl.fhirtoomop.model.omop.CareSite;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.metrics.BatchMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

/**
 * The FhirToOmopJobListener class describes activities to be performed before and after the
 * execution of the job.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class FhirToOmopJobListener implements JobExecutionListener {
  private final OmopRepository omopRepository;
  private final DataSource inputDataSource;
  private final DataSource outputDataSource;

  @Value("${data.fhirGateway.tableName}")
  private String inputTableName;

  @Value("${data.fhirGateway.jdbcUrl}")
  private String fhirJdbcUrl;

  @Value("${data.omopCdm.schema}")
  private String outputSchemaName;

  @Value("${data.omopCdm.jdbcUrl}")
  private String omopJdbcUrl;

  @Value("${app.version}")
  private String version;

  private Boolean bulkLoad;

  @Value("${data.beginDate}")
  private String beginDateStr;

  @Value("${data.endDate}")
  private String endDateStr;

  @Value("${data.fhirServer.baseUrl}")
  private String fhirBaseUrl;

  @Value("${app.startSingleStep}")
  private String startSingleStep;

  /**
   * Constructor for objects of the class FhirToOmopJobListener.
   *
   * @param omopRepository OMOP CDM repositories
   * @param inputDataSource connection data of source database
   * @param outputDataSource connection data of target database
   * @param bulkLoad flag to differentiate between bulk load or incremental load
   * @param beginDate date from which the data will be read
   * @param endDate date until which the data will be read
   */
  @Autowired
  public FhirToOmopJobListener(
      OmopRepository omopRepository,
      DataSource inputDataSource,
      DataSource outputDataSource,
      Boolean bulkLoad) {
    this.omopRepository = omopRepository;
    this.inputDataSource = inputDataSource;
    this.outputDataSource = outputDataSource;
    this.bulkLoad = bulkLoad;
  }

  /**
   * Executes all activities which should take place before the job is executed.
   *
   * @param jobExecution the execution of the job
   */
  @Override
  public void beforeJob(JobExecution jobExecution) {

    printDatabaseInfo();
    var beginJob = "==== Begin to transfer data from FHIR to OMOP ====";

    // if (ifBulkLoad.equals(Boolean.TRUE) && Strings.isNullOrEmpty(startSingleStep)) {
    //    insertCareSite(jobExecution);
    // }

    insertSourceToConceptMap(jobExecution);

    log.info("=".repeat(beginJob.length()));
    log.info(beginJob);
    log.info("=".repeat(beginJob.length()));
  }

  /** Print the Job meta information in the log. */
  private void printDatabaseInfo() {
    String printInfo = "==== FHIR-to-OMOP Job Infomation ====";
    log.info("=".repeat(printInfo.length()));
    log.info(printInfo);
    log.info("=".repeat(printInfo.length()));

    try (var inputConnection = inputDataSource.getConnection();
        var outputConnection = outputDataSource.getConnection()) {

      var format = "| %-40s | %-50s |";
      log.info("-".repeat(90));
      log.info(
          String.format(
              format,
              "This Job runs as",
              Boolean.FALSE.equals(bulkLoad) ? "Incremental Load" : "Bulk Load"));
      if (StringUtils.isBlank(fhirBaseUrl)) {
        log.info(String.format(format, "FHIR-Gateway URL", fhirJdbcUrl));
        log.info(String.format(format, "Retrieve FHIR Resources from table", inputTableName));
      } else {
        log.info(String.format(format, "FHIR Server URL", fhirBaseUrl));
      }

      log.info(
          String.format(
              format,
              "Retrieving FHIR Resources date range",
              "From " + beginDateStr + " to " + adjustedEndDate()));
      log.info(String.format(format, "OMOP Database URL", omopJdbcUrl));
      log.info(String.format(format, "Write data into Schema", outputSchemaName));
      log.info(String.format(format, "OMOP CDM Version", "v5.3.1"));
      log.info(String.format(format, "ETL Job version", version));
      log.info(String.format(format, "ETL Job starts at", LocalDate.now()));
      log.info("-".repeat(90));
      Thread.sleep(5000);
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public boolean checkGernerallInput() {
    boolean singleStepInputCheck = checkSingleStepInput(startSingleStep);
    boolean beginDateInputCheck = checkDateInput(beginDateStr);
    boolean endDateInputCheck = checkDateInput(endDateStr);

    if (!singleStepInputCheck) {
      loggingOutput("step name");

      return false;
    }

    if (!beginDateInputCheck) {
      loggingOutput("begin date");
      return false;
    }
    if (!endDateInputCheck) {
      loggingOutput("end date");
      return false;
    }
    return true;
  }

  private void loggingOutput(String paramterName) {

    var loggingBasis =
        String.format("The input [%s] is invalid. Please check your input string.", paramterName);
    var frame = "=".repeat(loggingBasis.length());
    log.error(frame);
    log.error(loggingBasis);
    log.error(frame);
  }

  /**
   * Check if the input step name is correct.
   *
   * @param singleStepName the Step name which should be run separately.
   * @return a boolean value
   */
  private boolean checkSingleStepInput(String singleStepName) {
    ArrayList<String> singleStepValues =
        new ArrayList<>(
            Arrays.asList(
                FHIR_RESOURCE_OBSERVATION,
                FHIR_RESOURCE_CONDITION,
                FHIR_RESOURCE_DEPARTMENT_CASE,
                FHIR_RESOURCE_MEDICATION,
                FHIR_RESOURCE_MEDICATION_ADMINISTRATION,
                FHIR_RESOURCE_MEDICATION_STATEMENT,
                FHIR_RESOURCE_PROCEDURE,
                FHIR_RESOURCE_IMMUNIZATION,
                FHIR_RESOURCE_CONSENT,
                FHIR_RESOURCE_DIAGNOSTIC_REPORT));

    return (Strings.isNullOrEmpty(singleStepName) || singleStepValues.contains(singleStepName));
  }

  /**
   * Check if the input date is correct.
   *
   * @param dateInput date from which the data will be read or date until which the data will be
   *     read
   * @return a boolean value
   */
  private boolean checkDateInput(String dateInput) {
    try {
      var date = LocalDate.parse(dateInput);
      return true;
    } catch (Exception e) {

    }
    return false;
  }

  /**
   * Adjust the end date in the reading frame for loging output
   *
   * @return adjusted end date as string
   */
  private String adjustedEndDate() {
    LocalDate end = LocalDate.parse(endDateStr);
    if (!end.isBefore(LocalDate.now())) {
      return LocalDate.now().toString();
    }
    return endDateStr;
  }

  /**
   * Executes all activities which should take place after the job has been executed.
   *
   * @param jobExecution the execution of the job
   */
  @Override
  public void afterJob(JobExecution jobExecution) {
    var exitStatus = jobExecution.getExitStatus();
    if (exitStatus.equals(ExitStatus.COMPLETED)) {
      logResults(jobExecution);
      logSkippedReasons(jobExecution);
      //    logSkippedReasons(jobExecution);
      var endJob = "==== Job End ====";
      log.info("=".repeat(endJob.length()));
      log.info(endJob);
      log.info("=".repeat(endJob.length()));
    }
  }

  /**
   * Log out the result of Job as a table.
   *
   * @param jobExecution the execution of the job
   */
  private void logResults(JobExecution jobExecution) {
    var totalReadCount = 0;
    var totalWriteCount = 0;
    var totalDeletedCount = 0;
    var totalSkippedCount = 0;
    var format = "| %-40s | %-12s | %-12s | %-12s | %-12s | %-20s |";
    var header =
        String.format(
            format,
            "Step",
            "Read Count",
            "Write Count",
            "Skipped Count",
            "Deleted Count",
            "Step Duration");
    var sepLine = "-".repeat(header.length());

    log.info("");
    log.info("==== Summary ====");
    log.info(sepLine);
    log.info(header);
    log.info(sepLine);
    List<String> ignoredSteps = Arrays.asList("stepPostProcess", "stepValidation", "initJobInfo");
    for (var stepExecution : jobExecution.getStepExecutions()) {
      Duration stepExecutionDuration =
          BatchMetrics.calculateDuration(stepExecution.getStartTime(), stepExecution.getEndTime());
      //      if (!stepExecution.getStepName().equalsIgnoreCase("stepPostProcess")) {
      if (!ignoredSteps.contains(stepExecution.getStepName())) {
        Integer readCount = stepExecution.getReadCount();
        Integer writeCount = stepExecution.getWriteCount();
        //      Integer skipedCount = readCount - writeCount;
        Integer deletedCount =
            (int)
                Metrics.globalRegistry
                    .counter("batch.fhir.resources.deleted", "deleted", stepExecution.getStepName())
                    .count();
        Integer skipedCount = stepExecution.getFilterCount() - deletedCount;
        log.info(
            String.format(
                format,
                stepExecution.getStepName(),
                readCount.toString(),
                writeCount.toString(),
                skipedCount.toString(),
                deletedCount.toString(),
                BatchMetrics.formatDuration(stepExecutionDuration)));
        totalReadCount += stepExecution.getReadCount();
        totalWriteCount += stepExecution.getWriteCount();
        totalDeletedCount += deletedCount;
        totalSkippedCount += skipedCount;
      }
      if (stepExecution.getStepName().equals("stepPostProcess")) {
        log.info(
            "[{}] has updated/written [{}] Rows in [{}].",
            stepExecution.getStepName(),
            stepExecution.getWriteCount(),
            BatchMetrics.formatDuration(stepExecutionDuration));
      }
    }

    log.info("Total Read FHIR Resources: {}", totalReadCount);
    log.info("Total Written FHIR Resources: {}", totalWriteCount);
    log.info("Total Skipped FHIR Resources: {}", totalSkippedCount);
    log.info("Total Deleted FHIR Resources: {}", totalDeletedCount);
  }

  /**
   * Log the reasons of skipped records as table.
   *
   * @param jobExecution the execution of the job
   */
  @Deprecated
  private void logSkippedReasons(JobExecution jobExecution) {
    var format = "| %-40s | %-40s | %-12s |";
    var header = String.format(format, "Step", "Skip Reason", "Skipped Count");
    var sepLine = "-".repeat(header.length());
    log.info(sepLine);
    log.info(header);
    log.info(sepLine);

    String previousStep = null; // To keep track of the previous step

    for (var counter : getCounters(jobExecution)) {
      if ((int) counter.count() == 0) {
        continue;
      }

      String currentStep = counter.getId().getTag("type"); // Current step

      // Print current step only if it's different from the previous one
      if (!Objects.equals(currentStep, previousStep)) {
        log.info(String.format(format, currentStep, counter.getId().getDescription(), (int) counter.count()));
      } else {
        // If it's the same step, print an empty string for the step column
        log.info(String.format(format, "", counter.getId().getDescription(), (int) counter.count()));
      }

      previousStep = currentStep; // Update previous step
    }
  }


  /**
   * Retrieve all Counters from global Metrics.
   *
   * @param jobExecution the execution of the job
   * @return a List of Counters.
   */
  private List<Counter> getCounters(JobExecution jobExecution) {
    List<Counter> counters = new ArrayList<>();
    for (var stepExecution : jobExecution.getStepExecutions()) {
      counters.add(Metrics.counter("no.person.id", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.start.date", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("invalid.code", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.source.code", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.fhir.reference", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("resource.status.error", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.matching.encounter", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("history.of.travel.result.not.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.acceptable.history.of.travel.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.available.info.history.of.travel.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.value.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.interpretation.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.reference.range.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("missing.high.range", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("missing.low.range", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("category.not.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("verification.status.not.acceptable", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("icd.code.invalid", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("diagnostic.confidence.not.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("status.not.acceptable", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("unable.extract.resource", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.matching.visitOccurence", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.start.date.found.in.location", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.location.reference.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.department.code.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("invalid.dose.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("invalid.dosage.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("invalid.route.counter", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("no.birth.date.found", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("invalid.string.length", "type", stepExecution.getStepName()));
      counters.add(Metrics.counter("invalid.birth.date", "type", stepExecution.getStepName()));
    }
    return counters;
  }

  /**
   * Fills the care_site table in OMOP CDM with data from a csv file.
   *
   * @param jobExecution the execution of the job
   */
  private void insertCareSite(JobExecution jobExecution) {
    log.info("Inserting data into CARE_SITE.");
    Resource careSite = new ClassPathResource("CARE_SITE.csv");
    try {
      //      omopRepository.getCareSiteRepository().truncateCareSite();
      jobExecution.setExitStatus(ExitStatus.COMPLETED);
    } catch (Exception e) {
      log.error("No file found in {}", careSite);
      jobExecution.setExitStatus(ExitStatus.FAILED);
    }

    try (BufferedReader br =
        new BufferedReader(new FileReader(careSite.getFile(), StandardCharsets.UTF_8))) {
      br.readLine();
      var line = "";
      List<CareSite> careSites = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        var splitedLine = Arrays.asList(line.split(";"));
        careSites.add(
            CareSite.builder()
                .careSiteId(Long.parseLong(splitedLine.get(0)))
                .careSiteName(splitedLine.get(1))
                .placeOfServiceConceptId(Integer.parseInt(splitedLine.get(2)))
                .careSiteSourceValue(splitedLine.get(4))
                .build());
      }

      omopRepository.getCareSiteRepository().saveAll(careSites);
      log.info("Inserted {} rows into CARE_SITE.", careSites.size());
    } catch (FileNotFoundException e) {
      log.error(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Fills the source_to_concept_map table in OMOP CDM with data from a csv file.
   *
   * @param jobExecution the execution of the job
   */
  private void insertSourceToConceptMap(JobExecution jobExecution) {

    log.info("Inserting data into SOURCE_TO_CONCEPT_MAP.");
    Resource sourceToConceptMap = new ClassPathResource("SOURCE_TO_CONCEPT_MAP.csv");
    try {
      omopRepository.getSourceToConceptRepository().deleteAll();
      jobExecution.setExitStatus(ExitStatus.COMPLETED);
    } catch (Exception e) {
      log.error("No file found in {}", sourceToConceptMap);
      jobExecution.setExitStatus(ExitStatus.FAILED);
    }

    var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    try (BufferedReader br =
        new BufferedReader(new FileReader(sourceToConceptMap.getFile(), StandardCharsets.UTF_8))) {
      br.readLine();
      var line = "";
      List<SourceToConceptMap> sourceToConceptMaps = new ArrayList<>();
      while ((line = br.readLine()) != null) {
        var splitedLine = Arrays.asList(line.split(";"));

        sourceToConceptMaps.add(
            SourceToConceptMap.builder()
                .sourceCode(splitedLine.get(0))
                .sourceConceptId(Integer.parseInt(splitedLine.get(1)))
                .sourceVocabularyId(splitedLine.get(2))
                .sourceCodeDescription(splitedLine.get(3))
                .targetConceptId(Integer.parseInt(splitedLine.get(4)))
                .targetVocabularyId(splitedLine.get(5))
                .validStartDate(LocalDate.parse(splitedLine.get(6), formatter))
                .validEndDate(LocalDate.parse(splitedLine.get(7), formatter))
                .build());
      }

      omopRepository.getSourceToConceptRepository().saveAll(sourceToConceptMaps);
      log.info("Inserted {} rows into SOURCE_TO_CONCEPT_MAP.", sourceToConceptMaps.size());
    } catch (FileNotFoundException e) {
      log.error(e.getMessage());
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
