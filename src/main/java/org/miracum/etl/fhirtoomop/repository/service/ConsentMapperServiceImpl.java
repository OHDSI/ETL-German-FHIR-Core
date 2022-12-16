package org.miracum.etl.fhirtoomop.repository.service;

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
@Service("ConsentServiceImpl")
public class ConsentMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;

  /**
   * Delete FHIR Observation resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Observation resource
   */
  public void deleteExistingConsentObservationByFhirLogicalId(String fhirLogicalId) {
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR Observation resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Observation resource
   */
  public void deleteExistingConsentObservationByFhirIdentifier(String fhirIdentifier) {
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
