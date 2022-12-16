package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import org.miracum.etl.fhirtoomop.model.omop.DrugExposure;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * The DrugExposureRepository interface represents a repository for the drug_exposure table in OMOP
 * CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface DrugExposureRepository extends JpaRepository<DrugExposure, Long> {

  /** Sets all visit_detail_id values in drug_exposure table in OMOP CDM to NULL. */
  @Modifying
  @Transactional
  @Query(value = "UPDATE drug_exposure SET visit_detail_id = NULL", nativeQuery = true)
  void updateVisitDetailId();

  @Modifying
  @Transactional
  @Query(
      value =
          "UPDATE drug_exposure SET visit_detail_id = NULL WHERE visit_detail_id in :visitDetailIds",
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
