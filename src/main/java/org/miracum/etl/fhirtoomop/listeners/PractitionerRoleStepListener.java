package org.miracum.etl.fhirtoomop.listeners;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.MemoryLogger;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.stereotype.Component;
import org.springframework.util.StopWatch;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.SQLException;

import static org.miracum.etl.fhirtoomop.Constants.*;

@Slf4j
@Component
@CacheConfig(cacheManager = "")
public class PractitionerRoleStepListener implements StepExecutionListener {

    private final OmopRepository repositories;
    private final DbMappings dbMappings;
    private final Boolean bulkload;

    private static final MemoryLogger memoryLogger = new MemoryLogger();
    private Boolean dictionaryLoadInRam;

    @Value("${app.startSingleStep}")
    private String startSingleStep;

    @Autowired
    @Qualifier("writerDataSource")
    private final DataSource dataSource;

    @Autowired
    public PractitionerRoleStepListener(
            OmopRepository repositories,
            DbMappings dbMappings,
            Boolean dictionaryLoadInRam,
            Boolean bulkload,
            DataSource dataSource
    ) {
        this.repositories = repositories;
        this.dbMappings = dbMappings;
        this.dictionaryLoadInRam = dictionaryLoadInRam;
        this.bulkload = bulkload;
        this.startSingleStep = startSingleStep;
        this.dataSource = dataSource;
    }


    @Override
    public void beforeStep(StepExecution stepExecution) {
        if (bulkload.equals(Boolean.TRUE)) {
            if (!Strings.isNullOrEmpty(startSingleStep)) {
                StopWatch stopWatch = new StopWatch();
                stopWatch.start();
                try {
                    deleteAllOrganizationReferenceData(stepExecution.createStepContribution());
                } catch (SQLException | IOException ex) {
                    ex.printStackTrace();
                    stepExecution.setTerminateOnly();
                }
                stopWatch.stop();

                log.info(
                        "Cleaned up all organization reference data data in [{}s]",
                        String.format("%.3f", stopWatch.getTotalTimeSeconds())
                );
            }
            if (dictionaryLoadInRam.equals(Boolean.TRUE)) {
                dbMappings.setFindPersonIdByLogicalId(
                        repositories.getPersonRepository().getFhirLogicalIdAndPersonId());
                dbMappings.setFindPersonIdByIdentifier(
                        repositories.getPersonRepository().getFhirIdentifierAndPersonId());

                dbMappings.setFindVisitOccIdByLogicalId(
                        repositories.getVisitOccRepository().getFhirLogicalIdAndVisitOccId());
                dbMappings.setFindVisitOccIdByIdentifier(
                        repositories.getVisitOccRepository().getFhirIdentifierAndVisitOccId());
            }

            dbMappings.setFindIcdSnomedMapping(repositories.getIcdSnomedRepository().getIcdSnomedMap());
            dbMappings.setFindOrphaSnomedMapping(
                    repositories.getOrphaSnomedMappingRepository().getOrphaSnomedMap());
            dbMappings
                    .getOmopConceptMapWrapper()
                    .setFindValidSnomedConcept(
                            repositories.getConceptRepository().findValidConceptId(VOCABULARY_SNOMED));
            dbMappings
                    .getOmopConceptMapWrapper()
                    .setFindValidIPRDConcept(
                            repositories.getConceptRepository().findValidConceptId(VOCABULARY_IPRD));
            dbMappings
                    .getOmopConceptMapWrapper()
                    .setFindValidIcd10GmConcept(
                            repositories.getConceptRepository().findValidConceptId(VOCABULARY_ICD10GM));
        }
        dbMappings.setFindHardCodeConcept(
                repositories.getSourceToConceptRepository().sourceToConceptMap());
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        memoryLogger.logMemoryDebugOnly();
        if (bulkload.equals(Boolean.TRUE)) {
            dbMappings.getFindIcdSnomedMapping().clear();
            dbMappings.getFindOrphaSnomedMapping().clear();
            if (dictionaryLoadInRam.equals(Boolean.TRUE)) {
                dbMappings.getFindPersonIdByIdentifier().clear();
                dbMappings.getFindPersonIdByLogicalId().clear();
                dbMappings.getFindVisitOccIdByIdentifier().clear();
                dbMappings.getFindVisitOccIdByLogicalId().clear();
            }
        }

        dbMappings.getFindHardCodeConcept().clear();
        return ExitStatus.COMPLETED;
    }


    private void deleteAllOrganizationReferenceData(StepContribution contribution) throws SQLException, IOException {

    }
}
