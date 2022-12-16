package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.DeviceExposureRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional("transactionManager")
@Service("DeviceExposureMapperServiceImpl")
public class DeviceExposureMapperServiceImpl {

  @Autowired private DeviceExposureRepository deviceExposureRepository;
  /**
   * Delete FHIR MedicationStatement and MedicationAdministration resources from OMOP CDM tables
   * using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR MedicationStatement or MedicationAdministration
   *     resource
   */
  public void deleteExistingDeviceExposureByFhirLogicalId(String fhirLogicalId) {
    deviceExposureRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR MedicationStatement and MedicationAdministration resources from OMOP CDM tables
   * using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR MedicationStatement or MedicationAdministration
   *     resource
   */
  public void deleteExistingDeviceExposureByFhirIdentifier(String fhirIdentifier) {
    deviceExposureRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
