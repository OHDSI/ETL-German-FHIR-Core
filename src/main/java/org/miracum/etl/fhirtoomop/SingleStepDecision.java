package org.miracum.etl.fhirtoomop;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONSENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DEPARTMENT_CASE;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DIAGNOSTIC_REPORT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_IMMUNIZATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_ADMINISTRATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_STATEMENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_PROCEDURE;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The SingleStepDecision class describes the user's decision whether to write all FHIR resources to
 * OMOP CDM or only certain FHIR resource types.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class SingleStepDecision implements JobExecutionDecider {
  @Value("${app.startSingleStep}")
  private String startSingleStep;

  /**
   * Decides the further processing logic depending on the FHIR resource types selected by the user
   * which should be transformed to OMOP CDM.
   *
   * @param jobExecution batch domain object representing the execution of a job
   * @param stepExecution batch domain object representation the execution of a step
   * @return the FlowExecutionStatus depending on the FHIR resource types selected by the user
   */
  @Override
  public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

    switch (startSingleStep) {
      case FHIR_RESOURCE_OBSERVATION:
        return new FlowExecutionStatus(FHIR_RESOURCE_OBSERVATION);
      case FHIR_RESOURCE_CONDITION:
        return new FlowExecutionStatus(FHIR_RESOURCE_CONDITION);
      case FHIR_RESOURCE_PROCEDURE:
        return new FlowExecutionStatus(FHIR_RESOURCE_PROCEDURE);
      case FHIR_RESOURCE_MEDICATION_ADMINISTRATION:
        return new FlowExecutionStatus(FHIR_RESOURCE_MEDICATION_ADMINISTRATION);
      case FHIR_RESOURCE_MEDICATION_STATEMENT:
        return new FlowExecutionStatus(FHIR_RESOURCE_MEDICATION_STATEMENT);
      case FHIR_RESOURCE_DEPARTMENT_CASE:
        return new FlowExecutionStatus(FHIR_RESOURCE_DEPARTMENT_CASE);
      case FHIR_RESOURCE_IMMUNIZATION:
        return new FlowExecutionStatus(FHIR_RESOURCE_IMMUNIZATION);
      case FHIR_RESOURCE_CONSENT:
        return new FlowExecutionStatus(FHIR_RESOURCE_CONSENT);
      case FHIR_RESOURCE_DIAGNOSTIC_REPORT:
        return new FlowExecutionStatus(FHIR_RESOURCE_DIAGNOSTIC_REPORT);
      case "":
        return new FlowExecutionStatus("All");
      default:
        log.warn(
            "==== The step [{}] cannot be run separately. Please try other steps. ====",
            startSingleStep);
        return FlowExecutionStatus.FAILED;
    }
  }
}
