package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.ConditionOccRepository;
import org.miracum.etl.fhirtoomop.repository.FactRelationshipRepository;
import org.miracum.etl.fhirtoomop.repository.MeasurementRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.miracum.etl.fhirtoomop.repository.PostProcessMapRepository;
import org.miracum.etl.fhirtoomop.repository.ProcedureOccRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ConditionMapperServiceImpl class contains the specific implementation to access data trough
 * OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("ConditionMapperServiceImpl")
public class ConditionMapperServiceImpl {
  @Autowired ConditionOccRepository conditionOccRepository;
  @Autowired ObservationRepository observationRepository;
  @Autowired MeasurementRepository measurementRepository;
  @Autowired ProcedureOccRepository procedureOccRepository;
  @Autowired FactRelationshipRepository frRepository;
  @Autowired PostProcessMapRepository ppmRepository;

  /**
   * Deletes FHIR Condition resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Condition resource
   */
  public void deleteExistingConditionsByFhirLogicalId(String fhirLogicalId) {
    procedureOccRepository.deleteByFhirLogicalId(fhirLogicalId);
    measurementRepository.deleteByFhirLogicalId(fhirLogicalId);
    conditionOccRepository.deleteByFhirLogicalId(fhirLogicalId);
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
    //    frRepository.deleteByFhirLogicalId1(fhirLogicalId);
    ppmRepository.deletePrimarySecondaryByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Deletes FHIR Condition resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Condition resource
   */
  public void deleteExistingConditionsByFhirIdentifier(String fhirIdentifier) {
    procedureOccRepository.deleteByFhirIdentifier(fhirIdentifier);
    measurementRepository.deleteByFhirIdentifier(fhirIdentifier);
    conditionOccRepository.deleteByFhirIdentifier(fhirIdentifier);
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
    //    frRepository.deleteByFhirIdentifier1(fhirIdentifier);
    ppmRepository.deletePrimarySecondaryByFhirIdentifier(fhirIdentifier);
  }

  /**
   * Deletes rank and use information from post_process_map table using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Condition resource
   */
  public void deleteExistingPpmEntriesByFhirLogicalId(String fhirLogicalId) {
    ppmRepository.deleteConditionByFhirLogicalId(fhirLogicalId + "%");
  }

  /**
   * Deletes rank and use information from post_process_map table using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Condition resource
   */
  public void deleteExistingPpmEntriesByFhirIdentifier(String fhirIdentifier) {
    ppmRepository.deleteConditionByFhirIdentifier("%:" + fhirIdentifier);
  }

  /**
   * Updates flag for rank and use information in post_process_map table using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Condition resource
   */
  public void updateExistingPpmEntriesByFhirLogicalId(String fhirLogicalId) {
    ppmRepository.updateDiagnosisByFhirLogicalId(fhirLogicalId + "%");
  }

  /**
   * Updates flag for rank and use information in post_process_map table using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Condition resource
   */
  public void updateExistingPpmEntriesByFhirIdentifier(String fhirIdentifier) {
    ppmRepository.updateDiagnosisByFhirIdentifier("%:" + fhirIdentifier);
  }
}
