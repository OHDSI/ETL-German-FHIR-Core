package org.miracum.etl.fhirtoomop.listeners;

import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;

import lombok.extern.slf4j.Slf4j;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.MemoryLogger;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.stereotype.Component;

/**
 * The PatientStepListener class describes activities to be performed before and after the execution
 * of the step for FHIR Patient resources.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
@CacheConfig(cacheManager = "caffeineCacheManager")
public class PatientStepListener implements StepExecutionListener {
  private final OmopRepository repositories;
  private final DbMappings dbMappings;
  private static final MemoryLogger memoryLogger = new MemoryLogger();
  private final Boolean bulkload;
  @Autowired CaffeineCacheManager cacheManager;

  /**
   * Constructor for objects of the class PatientStepListener.
   *
   * @param repositories OMOP CDM repositories
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param idMappings reference to internal id mappings
   * @param bulkload flag to differentiate between bulk load or incremental load
   */
  @Autowired
  public PatientStepListener(OmopRepository repositories, DbMappings dbMappings, Boolean bulkload) {
    this.repositories = repositories;
    this.dbMappings = dbMappings;
    this.bulkload = bulkload;
  }

  /**
   * Executes all activities which should take place before the step for FHIR Patient resources is
   * executed.
   *
   * @param stepExecution the execution of the step
   */
  @Override
  public void beforeStep(StepExecution stepExecution) {

    dbMappings.setFindHardCodeConcept(
        repositories.getSourceToConceptRepository().sourceToConceptMap());
    dbMappings
        .getOmopConceptMapWrapper()
        .setFindValidSnomedConcept(
            repositories.getConceptRepository().findValidConceptId(VOCABULARY_SNOMED));
    dbMappings.setFindSnomedRaceStandardMapping(
        repositories.getSnomedRaceRepository().getSnomedRaceMap());
  }

  /** Empties the complete OMOP CDM database. */
  private void truncateDb() {
    repositories.getPersonRepository().truncateTable();
    repositories.getLocationRepository().deleteAll();
    repositories.getPostProcessMapRepository().truncateTable();
    repositories.getFactRelationshipRepository().truncateTable();
    repositories.getMedicationIdRepository().deleteAll();
  }

  /**
   * Deletes all data from post_process_map table in OMOP CDM except diagnosis rank and use
   * information.
   */
  private void cleanUpTable() {
    repositories.getPostProcessMapRepository().deleteFromTable();
  }

  /**
   * Executes all activities which should take place after the step for FHIR Patient resources has
   * been executed.
   *
   * @param stepExecution the execution of the step
   * @return status of the step execution
   */
  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    memoryLogger.logMemoryDebugOnly();
    dbMappings.getFindHardCodeConcept().clear();
    cleanUpCache();
    return ExitStatus.COMPLETED;
  }

  /** Clean up CaffeineCache for patients. */
  private void cleanUpCache() {
    CaffeineCache patientLogicalIdCache =
        (CaffeineCache) cacheManager.getCache("patients-logicalid");
    CaffeineCache patientIdentifierCache =
        (CaffeineCache) cacheManager.getCache("patients-identifier");
    patientLogicalIdCache.clear();
    patientIdentifierCache.clear();
  }
}
