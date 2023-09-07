package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.AtcStandardDomainLookup;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The AtcStandardRepository interface represents a repository for the atc_standard_domain_lookup
 * view in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface AtcStandardRepository
    extends PagingAndSortingRepository<AtcStandardDomainLookup, Long> {

  /**
   * Retrieves a list of all records from atc_standard_domain_lookup view in OMOP CDM.
   *
   * @return list of all records from atc_standard_domain_lookup view in OMOP CDM
   */
  @Override
  List<AtcStandardDomainLookup> findAll();

  /**
   * Formats the list of all records from atc_standard_domain_lookup view in OMOP CDM as a map. The
   * source_code (ATC) is used as key.
   *
   * @return a map with all records from atc_standard_domain_lookup view using source_code as key
   */
  default Map<String, List<AtcStandardDomainLookup>> getAtcStandardMap() {
    return findAll().stream()
        .collect(Collectors.groupingBy(AtcStandardDomainLookup::getSourceCode));
  }

  /**
   * Retrieves a list of records from atc_standard_domain_lookup view in OMOP CDM based on a
   * specific source_code (ATC).
   *
   * @param sourceCode ATC code
   * @return list of records from atc_standard_domain_lookup view in OMOP CDM based on a specific
   *     source_code (ATC)
   */
  List<AtcStandardDomainLookup> findBySourceCode(String sourceCode);

  /**
   * Formats the list of records from atc_standard_domain_lookup view in OMOP CDM based on a
   * specific source_code (ATC) as a map. The source_code (ATC) is used as key.
   *
   * @param sourceCode ATC code
   * @return a map with all records from atc_standard_domain_lookup view in OMOP CDM based on a
   *     specific source_code (ATC)
   */
  default Map<String, List<AtcStandardDomainLookup>> getAtcStandardMapBySourceCode(
      String sourceCode) {
    return findBySourceCode(sourceCode).stream()
        .collect(Collectors.groupingBy(AtcStandardDomainLookup::getSourceCode));
  }
}
