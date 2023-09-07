package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.DrugExposureRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The MedicationAdministrationMapperServiceImpl class contains the specific implementation to
 * access data trough OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("MedicationAdministrationMapperServiceImpl")
public class MedicationAdministrationMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;
  @Autowired DrugExposureRepository drugExposureRepository;

  /**
   * Deletes FHIR MedicationAdministration resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR MedicationAdministration resource
   */
  public void deleteExistingMedAdsByFhirLogicalId(String fhirLogicalId) {
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
    drugExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Deletes FHIR MedicationAdministration resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR MedicationAdministration resource
   */
  public void deleteExistingMedAdsByFhirIdentifier(String fhirIdentifier) {
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
    drugExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
