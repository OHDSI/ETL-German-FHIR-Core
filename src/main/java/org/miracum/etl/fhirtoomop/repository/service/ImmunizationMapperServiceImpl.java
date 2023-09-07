package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.DrugExposureRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ImmunizationMapperServiceImpl class contains the specific implementation to access data
 * trough OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("ImmunizationMapperServiceImpl")
public class ImmunizationMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;
  @Autowired DrugExposureRepository drugExposureRepository;

  /**
   * Deletes FHIR Immunization resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Immunization resource
   */
  public void deleteExistingImmunizationByFhirLogicalId(String fhirLogicalId) {
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
    drugExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Deletes FHIR Immunization resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Immunization resource
   */
  public void deleteExistingImmunizationByFhirIdentifier(String fhirIdentifier) {
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
    drugExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
