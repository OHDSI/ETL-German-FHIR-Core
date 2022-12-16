package org.miracum.etl.fhirtoomop.repository;

import org.miracum.etl.fhirtoomop.model.omop.DeviceExposure;
import org.springframework.data.jpa.repository.JpaRepository;

public interface DeviceExposureRepository extends JpaRepository<DeviceExposure, Long> {
  /**
   * Delete entries in OMOP CDM table using fhir_logical_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   */
  void deleteByFhirLogicalId(String fhirLogicalId);

  /**
   * Delete entries in OMOP CDM table using fhir_identifier.
   *
   * @param fhirIdentifier identifier of the FHIR resource
   */
  void deleteByFhirIdentifier(String fhirIdentifier);
}
