package org.miracum.etl.fhirtoomop.repository;

import org.miracum.etl.fhirtoomop.model.omop.Death;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The DeathRepository interface represents a repository for the death table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface DeathRepository
    extends PagingAndSortingRepository<Death, Long>, JpaRepository<Death, Long> {

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
