package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.MeasurementRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
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
@Service("DiagnosticReportServiceImpl")
public class DiagnosticReportMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;
  @Autowired MeasurementRepository measurementRepository;
  @Autowired ProcedureOccRepository procedureOccRepository;

  /**
   * Deletes FHIR Condition resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Condition resource
   */
  public void deleteExistingDiagnosticReportByFhirLogicalId(String fhirLogicalId) {
    procedureOccRepository.deleteByFhirLogicalId(fhirLogicalId);
    measurementRepository.deleteByFhirLogicalId(fhirLogicalId);
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Deletes FHIR Condition resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Condition resource
   */
  public void deleteExistingDiagnosticReportByFhirIdentifier(String fhirIdentifier) {
    procedureOccRepository.deleteByFhirIdentifier(fhirIdentifier);
    measurementRepository.deleteByFhirIdentifier(fhirIdentifier);
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
