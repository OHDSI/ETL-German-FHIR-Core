package org.miracum.etl.fhirtoomop.repository;

import org.miracum.etl.fhirtoomop.model.omop.VisitDetail;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;

/**
 * The VisitDetailRepository interface represents a repository for the visit_detail table in OMOP
 * CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface VisitDetailRepository
    extends PagingAndSortingRepository<VisitDetail, Long>, JpaRepository<VisitDetail, Long> {

  /**
   * Sets all admitting_source_value, discharge_to_source_value, preceding_visit_detail_id and
   * visit_detail_parent_id values in visit_detail table in OMOP CDM to NULL, based on the
   * visit_occurrence_id.
   *
   * @param visitOccId the visit_occurrence_id in visit_detail table
   */
  @Modifying
  @Query(
      value =
          "UPDATE visit_detail SET admitting_source_value = NULL, discharge_to_source_value = NULL, preceding_visit_detail_id = NULL, visit_detail_parent_id = NULL WHERE visit_occurrence_id= :visitOccId",
      nativeQuery = true)
  void updateReferencedVisitDetailIds(@Param("visitOccId") Long visitOccId);

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

  @Query(value = "SELECT * FROM visit_detail where fhir_logical_id = :fhirLogicalId limit 1",
          nativeQuery = true)
  VisitDetail getStartDateOfVisitByFhirLogicalId(@Param("fhirLogicalId") String fhirLogicalId);

}
