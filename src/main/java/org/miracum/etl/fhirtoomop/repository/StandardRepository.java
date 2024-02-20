package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.StandardDomainLookup;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The LoincStandardRepository interface represents a repository for the
 * loinc_standard_domain_lookup view in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface StandardRepository
    extends PagingAndSortingRepository<StandardDomainLookup, Long> {

  /**
   * Retrieves a list of all records from loinc_standard_domain_lookup view in OMOP CDM.
   *
   * @return list of all records from loinc_standard_domain_lookup view in OMOP CDM
   */
  @Override
  List<StandardDomainLookup> findAll();

  /**
   * Formats the list of all records from loinc_standard_domain_lookup view in OMOP CDM as a map.
   * The source_code (LOINC) is used as key.
   *
   * @return a map with all records from loinc_standard_domain_lookup view using source_code as key
   */
  default Map<String, List<StandardDomainLookup>> getStandardMap() {
    return findAll().stream()
        .collect(Collectors.groupingBy(StandardDomainLookup::getSourceCode));
  }

  /**
   * Retrieves a list of records from loinc_standard_domain_lookup view in OMOP CDM based on a
   * specific source_code (LOINC).
   *
   * @param sourceCode LOINC code
   * @return list of records from loinc_standard_domain_lookup view in OMOP CDM based on a specific
   *     source_code (LOINC)
   */
  List<StandardDomainLookup> findBySourceCode(String sourceCode);

  /**
   * Formats the list of records from loinc_standard_domain_lookup view in OMOP CDM based on a
   * specific source_code (LOINC) as a map. The source_code (LOINC) is used as key.
   *
   * @param sourceCode LOINC code
   * @return a map with all records from loinc_standard_domain_lookup view in OMOP CDM based on a
   *     specific source_code (LOINC)
   */
  default Map<String, List<StandardDomainLookup>> getStandardMapBySourceCode(
      String sourceCode) {
    return findBySourceCode(sourceCode).stream()
        .collect(Collectors.groupingBy(StandardDomainLookup::getSourceCode));
  }
}
