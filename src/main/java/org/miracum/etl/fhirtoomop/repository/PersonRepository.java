package org.miracum.etl.fhirtoomop.repository;

import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.omop.Person;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The PersonRepository interface represents a repository for the person table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface PersonRepository
    extends PagingAndSortingRepository<Person, Long>, JpaRepository<Person, Long> {

  /** Empties the person table and its referencing tables in OMOP CDM. */
  @Transactional
  @Modifying
  @Query(value = "TRUNCATE TABLE person CASCADE", nativeQuery = true)
  void truncateTable();

  /**
   * Retrieves a list of all records from person table in OMOP CDM based on a specific
   * fhir_identifier.
   *
   * @param fhirIdentifier identifier for the source data in the FHIR resource
   * @return record from person table in OMOP CDM based on a specific fhir_identifier
   */
  Person findByFhirIdentifier(String fhirIdentifier);

  /**
   * Retrieves a list of all records from person table in OMOP CDM based on a specific
   * fhir_logical_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   * @return record from person table in OMOP CDM based on a specific fhir_logical_id
   */
  Person findByFhirLogicalId(String fhirLogicalId);

  /**
   * Formats the list of all records from person table in OMOP CDM as a map. The map contains the
   * assignment of fhir_logical_id to person_id.
   *
   * @return map containing the assignment of fhir_logical_id to person_id
   */
  default Map<String, Long> getFhirLogicalIdAndPersonId() {
    return findAll().stream()
        .filter(m -> m.getFhirLogicalId() != null)
        .collect(Collectors.toMap(Person::getFhirLogicalId, Person::getPersonId));
  }

  /**
   * Formats the list of all records from person table in OMOP CDM as a map. The map contains the
   * assignment of fhir_identifier to person_id.
   *
   * @return map containing the assignment of fhir_identifier to person_id
   */
  default Map<String, Long> getFhirIdentifierAndPersonId() {
    return findAll().stream()
        .filter(m -> m.getFhirIdentifier() != null)
        .collect(Collectors.toMap(Person::getFhirIdentifier, Person::getPersonId));
  }

  void deleteByFhirLogicalId(String fhirLogicalId);

  void deleteByFhirIdentifier(String fhirIdentifier);
}
