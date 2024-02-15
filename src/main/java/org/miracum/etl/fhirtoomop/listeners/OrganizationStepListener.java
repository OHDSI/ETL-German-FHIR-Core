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

import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_IPRD;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;

@Slf4j
@Component
@CacheConfig(cacheManager = "caffeineCacheManager")
public class OrganizationStepListener implements StepExecutionListener {

    private final OmopRepository repositories;
    private final DbMappings dbMappings;
    private static final MemoryLogger memoryLogger = new MemoryLogger();
    private final Boolean bulkload;
    @Autowired
    CaffeineCacheManager cacheManager;

    public OrganizationStepListener(OmopRepository repositories, DbMappings dbMappings, Boolean bulkload) {
        this.repositories = repositories;
        this.dbMappings = dbMappings;
        this.bulkload = bulkload;;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        if (bulkload.equals(Boolean.TRUE)) {
            log.info("========= Preparing OMOP DB for BulkLoad =========");
            truncateDb();
        } else {
            log.info("========= Preparing OMOP DB for IncrementalLoad =========");
            cleanUpTable();
        }
        dbMappings.setFindHardCodeConcept(
                repositories.getSourceToConceptRepository().sourceToConceptMap());
        dbMappings
                .getOmopConceptMapWrapper()
                .setFindValidSnomedConcept(
                        repositories.getConceptRepository().findValidConceptId(VOCABULARY_SNOMED));
        dbMappings.setFindSnomedRaceStandardMapping(
                repositories.getSnomedRaceRepository().getSnomedRaceMap());
        dbMappings
                .getOmopConceptMapWrapper()
                .setFindValidIPRDConcept(
                        repositories.getConceptRepository().findValidConceptId(VOCABULARY_IPRD));
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        memoryLogger.logMemoryDebugOnly();
        dbMappings.getFindHardCodeConcept().clear();
        cleanUpCache();
        return ExitStatus.COMPLETED;
    }

    private void truncateDb() {
        repositories.getCareSiteRepository().truncateTable();
        repositories.getLocationRepository().deleteAll();
        repositories.getPostProcessMapRepository().truncateTable();
        repositories.getFactRelationshipRepository().truncateTable();
        repositories.getMedicationIdRepository().deleteAll();
    }

    private void cleanUpCache() {
        CaffeineCache patientLogicalIdCache =
                (CaffeineCache) cacheManager.getCache("patients-logicalid");
        CaffeineCache patientIdentifierCache =
                (CaffeineCache) cacheManager.getCache("patients-identifier");
        patientLogicalIdCache.clear();
        patientIdentifierCache.clear();
    }
    private void cleanUpTable() {
        repositories.getPostProcessMapRepository().deleteFromTable();
    }
}
