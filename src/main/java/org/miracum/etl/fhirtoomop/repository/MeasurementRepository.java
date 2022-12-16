package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import org.miracum.etl.fhirtoomop.model.omop.Measurement;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The MeasurementRepository interface represents a repository for the measurement table in OMOP
 * CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Repository
public interface MeasurementRepository
    extends CrudRepository<Measurement, Long>, JpaRepository<Measurement, Long> {

  /**
   * Retrieves a list of all records from measurement table in OMOP CDM based on a specific
   * fhir_logical_id and excluding a specific measurement_type_concept_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   * @param typeConceptId foreign key to predefined Concept in the Standardized Vocabularies
   *     reflecting the provenance from where the Measurement record was recorded
   * @return list of all records from measurement table in OMOP CDM based on a specific
   *     fhir_logical_id and excluding a specific measurement_type_concept_id
   */
  List<Measurement> findByFhirLogicalIdAndMeasurementTypeConceptIdNot(
      String fhirLogicalId, int typeConceptId);

  /**
   * Retrieves a list of all records from measurement table in OMOP CDM based on a specific
   * fhir_identifier and excluding a specific measurement_type_concept_id.
   *
   * @param fhirIdentifier identifier for the source data in the FHIR resource
   * @param typeConceptId foreign key to predefined Concept in the Standardized Vocabularies
   *     reflecting the provenance from where the Measurement record was recorded
   * @return list of all records from measurement table in OMOP CDM based on a specific
   *     fhir_identifier and excluding a specific measurement_type_concept_id
   */
  List<Measurement> findByFhirIdentifierAndMeasurementTypeConceptIdNot(
      String fhirIdentifier, int typeConceptId);

  /** Sets all visit_detail_id values in measurement table in OMOP CDM to NULL. */
  @Modifying
  @Transactional
  @Query(value = "UPDATE measurement SET visit_detail_id = NULL", nativeQuery = true)
  void updateVisitDetailId();

  @Modifying
  @Transactional
  @Query(
      value =
          "UPDATE measurement SET visit_detail_id = NULL WHERE visit_detail_id in :visitDetailIds",
      nativeQuery = true)
  void updateVisitDetailIds(@Param("visitDetailIds") List<Long> visitDetailIds);

  /**
   * Deletes existing measurement records from the measurement table in OMOP CDM based on a existing
   * measurement_id.
   *
   * @param existingMeasurementId list of measurement_id for the existing record
   */
  @Modifying
  @Transactional
  @Query(
      value = "DELETE FROM measurement WHERE measurement_id in :existingMeasurementId",
      nativeQuery = true)
  void deleteExistingMeasurement(@Param("existingMeasurementId") List<Long> existingMeasurementId);

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
