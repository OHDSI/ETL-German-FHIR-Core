package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.DeviceExposureRepository;
import org.miracum.etl.fhirtoomop.repository.DrugExposureRepository;
import org.miracum.etl.fhirtoomop.repository.FactRelationshipRepository;
import org.miracum.etl.fhirtoomop.repository.MeasurementRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.miracum.etl.fhirtoomop.repository.ProcedureOccRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ProcedureMapperServiceImpl class contains the specific implementation to access data trough
 * OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("ProcedureMapperServiceImpl")
public class ProcedureMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;
  @Autowired MeasurementRepository measurementRepository;
  @Autowired ProcedureOccRepository procedureOccRepository;
  @Autowired DrugExposureRepository drugExposureRepository;
  @Autowired DeviceExposureRepository deviceExposureRepository;
  @Autowired FactRelationshipRepository factRelationshipRepository;

  /**
   * Delete FHIR Procedure resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Procedure resource
   */
  public void deleteExistingProcedureOccByFhirLogicalId(String fhirLogicalId) {
    procedureOccRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR Procedure resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Procedure resource
   */
  public void deleteExistingProcedureOccByFhirIdentifier(String fhirIdentifier) {
    procedureOccRepository.deleteByFhirIdentifier(fhirIdentifier);
  }

  /**
   * Deletes FHIR Procedure resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Procedure resource
   */
  public void deleteExistingProceduresByFhirLogicalId(String fhirLogicalId) {
    procedureOccRepository.deleteByFhirLogicalId(fhirLogicalId);
    measurementRepository.deleteByFhirLogicalId(fhirLogicalId);
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
    drugExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
    deviceExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
    factRelationshipRepository.deleteByFhirLogicalId1(fhirLogicalId);
  }

  /**
   * Deletes FHIR Procedure resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Procedure resource
   */
  public void deleteExistingProceduresByFhirIdentifier(String fhirIdentifier) {
    procedureOccRepository.deleteByFhirIdentifier(fhirIdentifier);
    measurementRepository.deleteByFhirIdentifier(fhirIdentifier);
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
    drugExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
    deviceExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
    factRelationshipRepository.deleteByFhirIdentifier1(fhirIdentifier);
  }
}
