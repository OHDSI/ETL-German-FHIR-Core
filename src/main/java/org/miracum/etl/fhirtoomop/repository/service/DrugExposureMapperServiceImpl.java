package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.DrugExposureRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The DrugExposureMapperServiceImpl class contains the specific implementation to access data
 * trough OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("DrugExposureMapperServiceImpl")
public class DrugExposureMapperServiceImpl {
  @Autowired DrugExposureRepository drugExposureRepository;

  /**
   * Delete FHIR MedicationStatement and MedicationAdministration resources from OMOP CDM tables
   * using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR MedicationStatement or MedicationAdministration
   *     resource
   */
  public void deleteExistingDrugExposureByFhirLogicalId(String fhirLogicalId) {
    drugExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR MedicationStatement and MedicationAdministration resources from OMOP CDM tables
   * using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR MedicationStatement or MedicationAdministration
   *     resource
   */
  public void deleteExistingDrugExposureByFhirIdentifier(String fhirIdentifier) {
    drugExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
