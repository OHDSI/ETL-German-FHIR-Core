package org.miracum.etl.fhirtoomop.listeners;

import lombok.extern.slf4j.Slf4j;
import org.miracum.etl.fhirtoomop.IIdMappings;
import org.miracum.etl.fhirtoomop.MemoryLogger;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The MedicationStepListener class describes activities to be performed before and after the
 * execution of the step for FHIR Medication resources.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class MedicationStepListener implements StepExecutionListener {
  private final OmopRepository repositories;
  private static final MemoryLogger memoryLogger = new MemoryLogger();
  private final IIdMappings idMappings;
  private final Boolean bulkload;

  @Value("${app.startSingleStep}")
  private String startSingleStep;

  /**
   * Constructor for objects of the class MedicationStepListener.
   *
   * @param repositories OMOP CDM repositories
   * @param startSingleStep parameter which indicates which steps should be executed
   * @param idMappings reference to internal id mappings
   * @param bulkload flag to differentiate between bulk load or incremental load
   */
  public MedicationStepListener(
      OmopRepository repositories, IIdMappings idMappings, Boolean bulkload) {
    this.repositories = repositories;
    this.idMappings = idMappings;
    this.bulkload = bulkload;
  }

  /**
   * Executes all activities which should take place before the step for FHIR Medication resources
   * is executed.
   *
   * @param stepExecution the execution of the step
   */
  @Override
  public void beforeStep(StepExecution stepExecution) {}

  /**
   * Executes all activities which should take place after the step for FHIR Medication resources
   * has been executed.
   *
   * @param stepExecution the execution of the step
   * @return status of the step execution
   */
  @Override
  public ExitStatus afterStep(StepExecution stepExecution) {
    memoryLogger.logMemoryDebugOnly();
    idMappings.getMedicationIds().clear();

    return ExitStatus.COMPLETED;
  }
}
