package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The SourceToConceptRepository interface represents a repository for the source_to_concept_map
 * table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface SourceToConceptRepository
    extends PagingAndSortingRepository<SourceToConceptMap, Long> {

  /**
   * Retrieves a list of all records from source_to_concept_map table in OMOP CDM.
   *
   * @return list of all records from source_to_concept_map table in OMOP CDM
   */
  @Override
  List<SourceToConceptMap> findAll();

  /**
   * Formats the list of all records from source_to_concept_map table in OMOP CDM as a map. The
   * source_vocabulary_id is used as key.
   *
   * @return a map with all records from source_to_concept_map table using source_vocabulary_id as
   *     key
   */
  default Map<String, List<SourceToConceptMap>> sourceToConceptMap() {
    return findAll().stream()
        .collect(Collectors.groupingBy(SourceToConceptMap::getSourceVocabularyId));
  }
}
