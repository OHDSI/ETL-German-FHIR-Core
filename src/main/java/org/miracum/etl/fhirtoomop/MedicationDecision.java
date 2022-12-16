package org.miracum.etl.fhirtoomop;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_STATEMENT;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The MedicationDecision class describes the switch between writing MedicationStatement resources
 * to OMOP CDM or skipping the processing of MedicationStatement resources.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Component
public class MedicationDecision implements JobExecutionDecider {
  @Value("${app.writeMedicationStatement.enabled}")
  private Boolean writeMedicationStatement;

  /**
   * Decides the further processing logic depending on the user's decision regarding the writing of
   * MedicationStatement resources to OMOP CDM.
   *
   * @param jobExecution batch domain object representing the execution of a job
   * @param stepExecution batch domain object representation the execution of a step
   * @return the FlowExecutionStatus for the user decision on MedicationStatement
   */
  @Override
  public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
    if (writeMedicationStatement.booleanValue()) {
      return new FlowExecutionStatus(FHIR_RESOURCE_MEDICATION_STATEMENT);
    }
    return new FlowExecutionStatus("SKIPPED");
  }
}
