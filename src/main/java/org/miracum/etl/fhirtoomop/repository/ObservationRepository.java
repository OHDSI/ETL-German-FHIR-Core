package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ObservationRepository interface represents a repository for the observation table in OMOP
 * CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Repository
public interface ObservationRepository
    extends PagingAndSortingRepository<OmopObservation, Long>,
        JpaRepository<OmopObservation, Long> {

  /**
   * Retrieves a list of all observation_ids from observation table in OMOP CDM based on a specific
   * fhir_logical_id and limited to ICD records and records with observation_type_concept_id =
   * 38000280.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   * @return list of all observation_ids from observation table in OMOP CDM based on a specific
   *     fhir_logical_id and limited to ICD records and records with observation_type_concept_id =
   *     38000280
   */
  @Query(
      value =
          "SELECT observation_id FROM observation where fhir_logical_id = :fhirLogicalId AND (observation_source_concept_id IN (SELECT concept_id FROM concept WHERE vocabulary_id = 'ICD10GM') OR observation_type_concept_id = 38000280)",
      nativeQuery = true)
  List<Long> getObservationIdByFhirLogicalId(@Param("fhirLogicalId") String fhirLogicalId);

  /**
   * Retrieves a list of all observation_ids from observation table in OMOP CDM based on a specific
   * fhir_identifier and limited to ICD records and records with observation_type_concept_id =
   * 38000280.
   *
   * @param fhirIdentifier identifier for the source data in the FHIR resource
   * @return list of all observation_ids from observation table in OMOP CDM based on a specific
   *     fhir_identifier and limited to ICD records and records with observation_type_concept_id =
   *     38000280
   */
  @Query(
      value =
          "SELECT observation_id FROM observation where fhir_identifier = :fhirIdentifier AND (observation_source_concept_id IN (SELECT concept_id FROM concept WHERE vocabulary_id = 'ICD10GM') OR observation_type_concept_id = 38000280)",
      nativeQuery = true)
  List<Long> getObservationIdByFhirIdentifier(@Param("fhirIdentifier") String fhirIdentifier);

  /** Sets all visit_detail_id values in observation table in OMOP CDM to NULL. */
  @Modifying
  @Transactional
  @Query(value = "UPDATE observation SET visit_detail_id = NULL", nativeQuery = true)
  void updateVisitDetailId();

  /**
   * Sets partially visit_detail_id values in observation table in OMOP CDM to NULL.
   *
   * @param visitDetailIds a list of visit_detail_ids
   */
  @Modifying
  @Transactional
  @Query(
      value =
          "UPDATE observation SET visit_detail_id = NULL WHERE visit_detail_id in :visitDetailIds",
      nativeQuery = true)
  void updateVisitDetailIds(@Param("visitDetailIds") List<Long> visitDetailIds);

  /**
   * Deletes existing observation records from the observation table in OMOP CDM based on a existing
   * observation_id.
   *
   * @param existingObservationId list of observation_id for the existing record
   */
  @Modifying
  @Transactional
  @Query(
      value = "DELETE FROM observation WHERE observation_id in :existingObservationId",
      nativeQuery = true)
  void deleteExistingObservation(@Param("existingObservationId") List<Long> existingObservationId);

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
