package org.miracum.etl.fhirtoomop.listeners;

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
 * The EncounterMainStepListener class describes activities to be performed before and after the
 * execution of the step for FHIR Encounter (administrative case/supply case) resources.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
@CacheConfig(cacheManager = "caffeineCacheManager")
public class EncounterMainStepListener implements StepExecutionListener {
  private final OmopRepository repositories;
  private final DbMappings dbMappings;
  private static final MemoryLogger memoryLogger = new MemoryLogger();
  private Boolean dictionaryLoadInRam;
  private final Boolean bulkload;
  @Autowired CaffeineCacheManager cacheManager;
  /**
   * Constructor for objects of the class EncounterMainStepListener.
   *
   * @param repositories OMOP CDM repositories
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param dictionaryLoadInRam parameter which indicates whether referenced data is searched in RAM
   *     or in OMOP CDM database
   * @param idMappings reference to internal id mappings
   * @param bulkload flag to differentiate between bulk load or incremental load
   */
  @Autowired
  public EncounterMainStepListener(
      OmopRepository repositories,
      DbMappings dbMappings,
      Boolean dictionaryLoadInRam,
      Boolean bulkload) {
    this.repositories = repositories;
    this.dbMappings = dbMappings;
    this.dictionaryLoadInRam = dictionaryLoadInRam;
    this.bulkload = bulkload;
  }

  /**
   * Executes all activities which should take place before the step for FHIR Encounter
   * (administrative case/supply case) resources is executed.
   *
   * @param stepExecution the execution of the step
   */
  @Override
  public void beforeStep(StepExecution stepExecution) {

    if (dictionaryLoadInRam.equals(Boolean.TRUE) && bulkload.equals(Boolean.TRUE)) {
      dbMappings.setFindPersonIdByLogicalId(
          repositories.getPersonRepository().getFhirLogicalIdAndPersonId());
      dbMappings.setFindPersonIdByIdentifier(
          repositories.getPersonRepository().getFhirIdentifierAndPersonId());
    }
    dbMappings.setFindHardCodeConcept(
        repositories.getSourceToConceptRepository().sourceToConceptMap());
    dbMappings.setFindCareSiteId(repositories.getCareSiteRepository().careSitesMap());
  }

  /**
   * Executes all activities which should take place after the step for FHIR Encounter
   * (administrative case/supply case) resources has been executed.
   *
   * @param stepExecution the execution of the step
   * @return status of the step execution
   */
  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    memoryLogger.logMemoryDebugOnly();
    if (dictionaryLoadInRam.equals(Boolean.TRUE) && bulkload.equals(Boolean.TRUE)) {
      dbMappings.getFindPersonIdByIdentifier().clear();
      dbMappings.getFindPersonIdByLogicalId().clear();
    }
    dbMappings.getFindHardCodeConcept().clear();
    cleanUpCache();

    return ExitStatus.COMPLETED;
  }

  /** Clean up CaffeineCache for patients and visits. */
  private void cleanUpCache() {
    CaffeineCache patientLogicalIdCache =
        (CaffeineCache) cacheManager.getCache("patients-logicalid");
    CaffeineCache patientIdentifierCache =
        (CaffeineCache) cacheManager.getCache("patients-identifier");
    patientLogicalIdCache.clear();
    patientIdentifierCache.clear();

    CaffeineCache visitLogicalIdCache = (CaffeineCache) cacheManager.getCache("visits-logicalid");
    CaffeineCache visitIdentifierCache = (CaffeineCache) cacheManager.getCache("visits-identifier");
    visitLogicalIdCache.clear();
    visitIdentifierCache.clear();
  }
}
