package org.miracum.etl.fhirtoomop.repository;

import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.omop.VisitOccurrence;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The VisitOccRepository interface represents a repository for the visit_occurrence table in OMOP
 * CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface VisitOccRepository
    extends PagingAndSortingRepository<VisitOccurrence, Long>,
        JpaRepository<VisitOccurrence, Long> {

  /**
   * Retrieves a list of all records from visit_occurrence table in OMOP CDM based on a specific
   * fhir_identifier.
   *
   * @param fhirIdentifier identifier for the source data in the FHIR resource
   * @return list of all records from visit_occurrence table in OMOP CDM based on a specific
   *     fhir_identifier
   */
  VisitOccurrence findByFhirIdentifier(String fhirIdentifier);

  /**
   * Retrieves a list of all records from visit_occurrence table in OMOP CDM based on a specific
   * fhir_logical_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   * @return list of all records from visit_occurrence table in OMOP CDM based on a specific
   *     fhir_logical_id
   */
  VisitOccurrence findByFhirLogicalId(String fhirLogicalId);

  /**
   * Formats the list of all records from concept table in OMOP CDM as a map. The map contains the
   * assignment of fhir_logical_id to visit_occurrence_id.
   *
   * @return map containing the assignment of fhir_logical_id to visit_occurrence_id
   */
  default Map<String, Long> getFhirLogicalIdAndVisitOccId() {
    return findAll().stream()
        .filter(m -> m.getFhirLogicalId() != null)
        .collect(
            Collectors.toMap(
                VisitOccurrence::getFhirLogicalId, VisitOccurrence::getVisitOccurrenceId));
  }

  /**
   * Formats the list of all records from concept table in OMOP CDM as a map. The map contains the
   * assignment of fhir_identifier to visit_occurrence_id.
   *
   * @return map containing the assignment of fhir_identifier to visit_occurrence_id
   */
  default Map<String, Long> getFhirIdentifierAndVisitOccId() {
    return findAll().stream()
        .filter(m -> m.getFhirIdentifier() != null)
        .collect(
            Collectors.toMap(
                VisitOccurrence::getFhirIdentifier, VisitOccurrence::getVisitOccurrenceId));
  }

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
