package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.DrugExposureRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The MedicationStatementMapperServiceImpl class contains the specific implementation to access
 * data trough OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("MedicationStatementMapperServiceImpl")
public class MedicationStatementMapperServiceImpl {
  @Autowired ObservationRepository observationRepository;
  @Autowired DrugExposureRepository drugExposureRepository;

  /**
   * Deletes FHIR MedicationStatement resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR MedicationStatement resource
   */
  public void deleteExistingMedStatsByFhirLogicalId(String fhirLogicalId) {
    observationRepository.deleteByFhirLogicalId(fhirLogicalId);
    drugExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Deletes FHIR MedicationStatement resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR MedicationStatement resource
   */
  public void deleteExistingMedStatsByFhirIdentifier(String fhirIdentifier) {
    observationRepository.deleteByFhirIdentifier(fhirIdentifier);
    drugExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
