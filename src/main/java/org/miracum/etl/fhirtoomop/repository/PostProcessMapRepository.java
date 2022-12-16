package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import javax.transaction.Transactional;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;

/**
 * The PostProcessMapRepository interface represents a repository for the post_process_map table in
 * OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface PostProcessMapRepository extends CrudRepository<PostProcessMap, Long> {

  /** Empties the post_process_map table in OMOP CDM. */
  @Transactional
  @Modifying
  @Query(value = "TRUNCATE TABLE cds_etl_helper.post_process_map", nativeQuery = true)
  void truncateTable();

  /** Empties the post_process_map table in OMOP CDM except diagnosis rank and use information. */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE omop_table NOT IN ('rank', 'use', 'primary_secondary_icd')",
      nativeQuery = true)
  void deleteFromTable();

  /**
   * Deletes diagnosis rank and use information from post_process_map table in OMOP CDM for a
   * specific encounter fhir_logical_id.
   */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE (omop_table='rank' OR omop_table='use') AND (fhir_logical_id = :fhirLogicalId)",
      nativeQuery = true)
  void deleteEncounterByFhirLogicalId(@Param("fhirLogicalId") String fhirLogicalId);

  /**
   * Deletes diagnosis rank and use information from post_process_map table in OMOP CDM for a
   * specific encounter fhir_identifier.
   */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE (omop_table='rank' OR omop_table='use') AND (fhir_identifier = :fhirIdentifier)",
      nativeQuery = true)
  void deleteEncounterByFhirIdentifier(@Param("fhirIdentifier") String fhirIdentifier);

  /**
   * Deletes diagnosis rank and use information from post_process_map table in OMOP CDM for a
   * specific condition fhir_logical_id.
   */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE (omop_table='use' or omop_table='rank') "
              + " AND data_one LIKE :fhirLogicalId",
      nativeQuery = true)
  void deleteConditionByFhirLogicalId(@Param("fhirLogicalId") String fhirLogicalId);

  /**
   * Deletes diagnosis rank and use information from post_process_map table in OMOP CDM for a
   * specific condition fhir_identifier.
   */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE (omop_table='use' or omop_table='rank') "
              + " AND data_one LIKE :fhirIdentifier",
      nativeQuery = true)
  void deleteConditionByFhirIdentifier(@Param("fhirIdentifier") String fhirIdentifier);

  /**
   * Updates the written-flag of diagnosis rank and use information in post_process_map table in
   * OMOP CDM for a specific condition fhir_logical_id.
   */
  @Modifying
  @Query(
      value =
          "UPDATE cds_etl_helper.post_process_map SET omop_id = 0 WHERE (omop_table='use' or omop_table='rank') "
              + "AND data_one LIKE :fhirLogicalId",
      nativeQuery = true)
  void updateDiagnosisByFhirLogicalId(@Param("fhirLogicalId") String fhirLogicalId);

  /**
   * Updates the written-flag of diagnosis rank and use information in post_process_map table in
   * OMOP CDM for a specific condition fhir_identifier.
   */
  @Modifying
  @Query(
      value =
          "UPDATE cds_etl_helper.post_process_map SET omop_id = 0 WHERE (omop_table='use' or omop_table='rank') "
              + "AND data_one LIKE :fhirIdentifier",
      nativeQuery = true)
  void updateDiagnosisByFhirIdentifier(@Param("fhirIdentifier") String fhirIdentifier);

  /**
   * Deletes primary secondary information from post_process_map table in OMOP CDM for a specific
   * condition fhir_logical_id.
   */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE omop_table='primary_secondary_icd' "
              + " AND fhir_logical_id =:fhirLogicalId",
      nativeQuery = true)
  void deletePrimarySecondaryByFhirLogicalId(@Param("fhirLogicalId") String fhirLogicalId);

  /**
   * Deletes diagnosis rank and use information from post_process_map table in OMOP CDM for a
   * specific condition fhir_identifier.
   */
  @Transactional
  @Modifying
  @Query(
      value =
          "DELETE FROM cds_etl_helper.post_process_map WHERE omop_table='primary_secondary_icd' "
              + " AND fhir_identifier =:fhirIdentifier",
      nativeQuery = true)
  void deletePrimarySecondaryByFhirIdentifier(@Param("fhirIdentifier") String fhirIdentifier);

  /**
   * Updates the written-flag of primary secondary ICD information in post_process_map table in OMOP
   * CDM for a list of specific condition fhir_logical_ids.
   */
  @Modifying
  @Query(
      value =
          "UPDATE cds_etl_helper.post_process_map SET omop_id = 0 WHERE omop_table='primary_secondary_icd' "
              + "AND fhir_logical_id IN :fhirLogicalIds",
      nativeQuery = true)
  void updatePrimarySecondaryByFhirLogicalId(@Param("fhirLogicalIds") List<String> fhirLogicalIds);
}
