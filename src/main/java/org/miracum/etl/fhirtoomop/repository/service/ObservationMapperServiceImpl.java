package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.MeasurementRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ObservationMapperServiceImpl class contains the specific implementation to access data trough
 * OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("OmopObservationServiceImpl")
public class ObservationMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;
  @Autowired MeasurementRepository measurementRepository;

  /**
   * Delete FHIR Observation resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Observation resource
   */
  public void deleteExistingLabObservationByFhirLogicalId(String fhirLogicalId) {
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
    measurementRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR Observation resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Observation resource
   */
  public void deleteExistingLabObservationByFhirIdentifier(String fhirIdentifier) {
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
    measurementRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
