package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import org.miracum.etl.fhirtoomop.model.omop.ConditionOccurrence;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ConditionOccRepository interface represents a repository for the condition_occurrence table
 * in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface ConditionOccRepository
    extends PagingAndSortingRepository<ConditionOccurrence, Long>,
        JpaRepository<ConditionOccurrence, Long> {

  /** Sets all visit_detail_id values in condition_occurrence table in OMOP CDM to NULL. */
  @Modifying
  @Transactional
  @Query(value = "UPDATE condition_occurrence SET visit_detail_id = NULL", nativeQuery = true)
  void updateVisitDetailId();

  /**
   * Sets visit_detail_id to NULL in condition_occurrence table.
   *
   * @param visitDetailIds a list of visit_detail_id for which should set to NULL
   */
  @Modifying
  @Transactional
  @Query(
      value =
          "UPDATE condition_occurrence SET visit_detail_id = NULL WHERE visit_detail_id IN :visitDetailIds",
      nativeQuery = true)
  void updateVisitDetailIds(@Param("visitDetailIds") List<Long> visitDetailIds);

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
