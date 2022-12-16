package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import org.miracum.etl.fhirtoomop.model.omop.FactRelationship;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

/**
 * The FactRelationshipRepository interface represents a repository for the fact_relationship table
 * in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface FactRelationshipRepository extends CrudRepository<FactRelationship, String> {

  /** Empties the fact_relationship table in OMOP CDM. */
  @Modifying
  @Transactional
  @Query(value = "TRUNCATE TABLE fact_relationship", nativeQuery = true)
  void truncateTable();

  /**
   * Delete entries in fact_relationship table using fhir_logical_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   */
  void deleteByFhirLogicalId1(String fhirLogicalId);

  /**
   * Delete entries in fact_relationship table using fhir_identifier.
   *
   * @param fhirIdentifier identifier of the FHIR resource
   */
  void deleteByFhirIdentifier1(String fhirIdentifier);

  /**
   * Deletes primary secondary ICD information from the fact_relationship table in OMOP CDM based on
   * a list of specific condition fhir_logical_ids.
   */
  @Modifying
  @Query(
      value =
          "DELETE FROM fact_relationship WHERE fhir_logical_id_1 IN :fhirLogicalIds AND (relationship_concept_id = 44818770 or relationship_concept_id = 44818868)",
      nativeQuery = true)
  void deletePrimarySecondaryByFhirLogicalId(@Param("fhirLogicalIds") List<String> fhirLogicalIds);
}
