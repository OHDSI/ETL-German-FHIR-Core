package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The ConceptRepository interface represents a repository for the concept table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface ConceptRepository extends PagingAndSortingRepository<Concept, Long> {

  /**
   * Retrieves a list of all records from concept table in OMOP CDM based on a specific
   * vocabulary_id.
   *
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     Concept has been adapted
   * @return a list of all records from concept table in OMOP CDM based on a specific vocabulary_id
   */
  List<Concept> findByVocabularyId(String vocabularyId);

  /**
   * Formats the list of all records from concept table in OMOP CDM based on a specific
   * vocabulary_id as a map. The concept_code is used as key.
   *
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     Concept has been adapted
   * @return a map with all records from concept table in OMOP CDM based on a specific vocabulary_id
   */
  default Map<String, List<Concept>> findValidConceptId(String vocabularyId) {
    return findByVocabularyId(vocabularyId).stream()
        .collect(Collectors.groupingBy(Concept::getConceptCode));
  }

  /**
   * Retrieves a list of records from concept table in OMOP CDM based on a specific vocabulary_id
   * and concept_code.
   *
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     Concept has been adapted
   * @param conceptCode the identifier of the Concept in the source vocabulary
   * @return list of records from concept table in OMOP CDM based on a specific vocabulary_id and
   *     concept_code
   */
  List<Concept> findByVocabularyIdAndConceptCode(String vocabularyId, String conceptCode);

  /**
   * Formats the page of records from concept table in OMOP CDM based on a specific vocabulary_id
   * and concept_code as a map. The concept_code is used as key.
   *
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     Concept has been adapted
   * @param conceptCode the identifier of the Concept in the source vocabulary
   * @return a map with all records from concept table in OMOP CDM based on a specific vocabulary_id
   *     and concept_code
   */
  default Map<String, List<Concept>> getConceptByVocabularyAndCode(
      String vocabularyId, String conceptCode) {
    return findByVocabularyIdAndConceptCode(vocabularyId, conceptCode).stream()
        .collect(Collectors.groupingBy(Concept::getConceptCode));
  }

  /**
   * Retrieves a list of records from concept table in OMOP CDM based on a specific concept_name and
   * vocabulary_id.
   *
   * @param conceptName unambiguous, meaningful and descriptive name for the Concept
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     Concept has been adapted
   * @return list of records from concept table in OMOP CDM based on a specific concept_name and
   *     vocabulary_id
   */
  List<Concept> findByConceptNameAndVocabularyId(String conceptName, String vocabularyId);

  /**
   * Formats the list of records from concept table in OMOP CDM based on a specific concept_name and
   * vocabulary_id as a map. The map contains the assignment of concept_name to concept_id.
   *
   * @param conceptName unambiguous, meaningful and descriptive name for the Concept
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     Concept has been adapted
   * @return map containing the assignment of concept_name to concept_id
   */
  default Map<String, Integer> findConceptByConceptName(String conceptName, String vocabularyId) {
    return findByConceptNameAndVocabularyId(conceptName, vocabularyId).stream()
        .collect(Collectors.toMap(Concept::getConceptName, Concept::getConceptId));
  }
}
