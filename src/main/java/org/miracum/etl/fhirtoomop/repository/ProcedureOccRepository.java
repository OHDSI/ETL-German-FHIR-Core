package org.miracum.etl.fhirtoomop.repository;

import org.miracum.etl.fhirtoomop.model.omop.ProcedureOccurrence;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ProcedureOccRepository interface represents a repository for the procedure_occurrence table
 * in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional
public interface ProcedureOccRepository
    extends PagingAndSortingRepository<ProcedureOccurrence, Long>,
        JpaRepository<ProcedureOccurrence, Long> {

  /** Sets all visit_detail_id values in procedure_occurrence table in OMOP CDM to NULL. */
  @Modifying
  @Query(value = "UPDATE procedure_occurrence SET visit_detail_id = NULL", nativeQuery = true)
  void updateVisitDetailId();

  /**
   * Delete entries in OMOP CDM table using fhir_logical_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   */
  //  @Modifying
  //  @Query(
  //      value = "DELETE FROM procedure_occurrence WHERE fhir_logical_id= :logicalId",
  //      nativeQuery = true)
  void deleteByFhirLogicalId(String fhirLogicalId);

  /**
   * Delete entries in OMOP CDM table using fhir_identifier.
   *
   * @param fhirIdentifier identifier of the FHIR resource
   */
  void deleteByFhirIdentifier(String fhirIdentifier);
}
