package org.miracum.etl.fhirtoomop;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * The CustomDecision class describes the switch between the two loading options bulk load or
 * incremental load of the ETL process.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Component
public class CustomDecision implements JobExecutionDecider {
  @Value("${app.bulkload.enabled}")
  private Boolean ifBulkLoad;

  private String bulkload = "BULKLOAD";
  private String incrementalLoad = "INCREMENTALLOAD";

  /**
   * Decides the further processing logic depending on the user's decision to load option.
   *
   * @param jobExecution batch domain object representing the execution of a job
   * @param stepExecution batch domain object representation the execution of a step
   * @return the FlowExecutionStatus for bulk load or incremental load
   */
  @Override
  public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

    if (ifBulkLoad.equals(Boolean.TRUE)) {
      jobExecution.setExitStatus(new ExitStatus(bulkload));
      return new FlowExecutionStatus(bulkload);
    } else {
      jobExecution.setExitStatus(new ExitStatus(incrementalLoad));
      return new FlowExecutionStatus(incrementalLoad);
    }
  }
}
