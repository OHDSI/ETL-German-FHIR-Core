package org.miracum.etl.fhirtoomop;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DEPARTMENT_CASE;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_IMMUNIZATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_ADMINISTRATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_STATEMENT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_PROCEDURE;

import java.io.IOException;
import java.sql.SQLException;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.miracum.etl.fhirtoomop.utils.ExecuteSqlScripts;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

/**
 * The PostProcessTask class represents the post processing step at database level, which takes
 * place after all FHIR resources are written to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class PostProcessTask implements Tasklet {
  private final DataSource dataSource;
  private final String startSingleStep;
  private final Boolean ifBulkLoad;

  /**
   * Constructor for objects of the class PostProcessTask.
   *
   * @param dataSource the data source to query against
   * @param startSingleStep parameter which indicates which steps should be executed
   * @param ifBulkLoad parameter which indicates the user selected loading option
   */
  public PostProcessTask(DataSource dataSource, String startSingleStep, Boolean ifBulkLoad) {
    this.dataSource = dataSource;
    this.startSingleStep = startSingleStep;
    this.ifBulkLoad = ifBulkLoad;
  }

  /**
   * Executes SQL files for post processing at database level.
   *
   * @param contribution buffers changes until they can be applied to a chunk boundary
   * @param chunkContext context object for weakly typed data stored for the duration of a chunk
   * @return the processing status
   * @throws SQLException
   * @throws IOException
   */
  @Override
  public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext)
      throws SQLException, IOException {
    ExecuteSqlScripts executeSqlScripts = new ExecuteSqlScripts(dataSource, contribution);

    Resource observationPeriod =
        new ClassPathResource("post_processing/post_process_observation_period.sql");
    Resource primarySecondaryIcd =
        new ClassPathResource("post_processing/post_process_primary_secondary_icd.sql");
    Resource icdSiteLocalization =
        new ClassPathResource("post_processing/post_process_icd_site_localization.sql");
    Resource severity = new ClassPathResource("post_processing/post_process_severity.sql");
    Resource stage = new ClassPathResource("post_processing/post_process_stage.sql");
    Resource diagnosisRank =
        new ClassPathResource("post_processing/post_process_diagnosis_rank.sql");
//    Resource visitDetailInOtherTable =
//            new ClassPathResource("post_processing/post_process_visit_detail_in_other_table.sql");
    Resource visitDetailUpdates =
        new ClassPathResource("post_processing/post_process_visit_detail_updates.sql");
//    Resource locationUpdates =
//        new ClassPathResource("post_processing/post_process_location_updates.sql");
    Resource visitAdmDis = new ClassPathResource("post_processing/post_process_visit_adm_dis.sql");
    Resource death = new ClassPathResource("post_processing/post_process_death.sql");
    Resource cleanUpFactRelationship =
        new ClassPathResource("post_processing/post_process_fact_relationship_clean_up.sql");
    Resource deviceProcedure =
        new ClassPathResource("post_processing/post_process_device_procedure.sql");
    Resource diagnoseUse = new ClassPathResource("post_processing/post_process_diagnosis_use.sql");
    Resource setCalculatedBirthOfYear =
        new ClassPathResource("post_processing/post_process_calculated_year_of_birth.sql");
    Resource datesOfHistoryOfTravel =
        new ClassPathResource("post_processing/post_process_history_of_travel.sql");
    Resource conditionEra = new ClassPathResource("post_processing/post_process_condition_era.sql");
    Resource drugEra = new ClassPathResource("post_processing/post_process_drug_era.sql");
    Resource endLoad = new ClassPathResource("post_processing/post_process_drug_era.sql");
    Resource practOrgRef = new ClassPathResource("post_processing/post_process_practitioner_organization_reference.sql");

    if (ifBulkLoad.equals(Boolean.FALSE)) {
      executeSqlScripts.executeSQLScript(observationPeriod);
//      executeSqlScripts.executeSQLScript(locationUpdates);
      executeSqlScripts.executeSQLScript(visitAdmDis);
      executeSqlScripts.executeSQLScript(visitDetailUpdates);
//       executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
      executeSqlScripts.executeSQLScript(death);
      executeSqlScripts.executeSQLScript(deviceProcedure);
//       executeSqlScripts.executeSQLScript(diagnoseUse);
//       executeSqlScripts.executeSQLScript(diagnosisRank);
      executeSqlScripts.executeSQLScript(icdSiteLocalization);
      executeSqlScripts.executeSQLScript(severity);
      executeSqlScripts.executeSQLScript(stage);
      executeSqlScripts.executeSQLScript(primarySecondaryIcd);
      executeSqlScripts.executeSQLScript(cleanUpFactRelationship);
      executeSqlScripts.executeSQLScript(setCalculatedBirthOfYear);
      executeSqlScripts.executeSQLScript(datesOfHistoryOfTravel);
      executeSqlScripts.executeSQLScript(conditionEra);
      executeSqlScripts.executeSQLScript(drugEra);
    } else {

      switch (startSingleStep) {
        case FHIR_RESOURCE_OBSERVATION:
//          executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
          executeSqlScripts.executeSQLScript(datesOfHistoryOfTravel);
          break;
        case FHIR_RESOURCE_CONDITION:
//          executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
          executeSqlScripts.executeSQLScript(diagnoseUse);
          executeSqlScripts.executeSQLScript(diagnosisRank);
          executeSqlScripts.executeSQLScript(icdSiteLocalization);
          executeSqlScripts.executeSQLScript(severity);
          executeSqlScripts.executeSQLScript(stage);
          executeSqlScripts.executeSQLScript(primarySecondaryIcd);
          executeSqlScripts.executeSQLScript(cleanUpFactRelationship);
          executeSqlScripts.executeSQLScript(conditionEra);

          break;
        case FHIR_RESOURCE_PROCEDURE:
          executeSqlScripts.executeSQLScript(deviceProcedure);
//          executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
          break;
        case FHIR_RESOURCE_MEDICATION_ADMINISTRATION, FHIR_RESOURCE_MEDICATION_STATEMENT:
          executeSqlScripts.executeSQLScript(drugEra);
//          executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
          break;

        case FHIR_RESOURCE_IMMUNIZATION:
          executeSqlScripts.executeSQLScript(drugEra);
          break;
        case FHIR_RESOURCE_DEPARTMENT_CASE:
          executeSqlScripts.executeSQLScript(visitDetailUpdates);
//          executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
          break;
        case "":
          executeSqlScripts.executeSQLScript(observationPeriod);
//          executeSqlScripts.executeSQLScript(locationUpdates);
          executeSqlScripts.executeSQLScript(visitAdmDis);
          executeSqlScripts.executeSQLScript(visitDetailUpdates);
//          executeSqlScripts.executeSQLScript(visitDetailInOtherTable);
          executeSqlScripts.executeSQLScript(death);
          executeSqlScripts.executeSQLScript(deviceProcedure);
//          executeSqlScripts.executeSQLScript(diagnoseUse);
//          executeSqlScripts.executeSQLScript(diagnosisRank);
          executeSqlScripts.executeSQLScript(icdSiteLocalization);
          executeSqlScripts.executeSQLScript(severity);
          executeSqlScripts.executeSQLScript(stage);
          executeSqlScripts.executeSQLScript(primarySecondaryIcd);
          executeSqlScripts.executeSQLScript(setCalculatedBirthOfYear);
          executeSqlScripts.executeSQLScript(datesOfHistoryOfTravel);
          executeSqlScripts.executeSQLScript(conditionEra);
          executeSqlScripts.executeSQLScript(drugEra);
          executeSqlScripts.executeSQLScript(practOrgRef);
//          executeSqlScripts.executeSQLScript(endLoad);
          break;
        default:
          return RepeatStatus.FINISHED;
      }
    }
    return RepeatStatus.FINISHED;
  }
}
