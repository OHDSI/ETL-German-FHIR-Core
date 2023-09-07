package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.OpsStandardDomainLookup;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The OpsStandardRepository interface represents a repository for the ops_standard_domain_lookup
 * view in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface OpsStandardRepository
    extends PagingAndSortingRepository<OpsStandardDomainLookup, Long> {

  /**
   * Retrieves a list of all records from ops_standard_domain_lookup view in OMOP CDM.
   *
   * @return list of all records from ops_standard_domain_lookup view in OMOP CDM
   */
  @Override
  List<OpsStandardDomainLookup> findAll();

  /**
   * Formats the list of all records from ops_standard_domain_lookup view in OMOP CDM as a map. The
   * source_code (OPS) is used as key.
   *
   * @return a map with all records from ops_standard_domain_lookup view using source_code as key
   */
  default Map<String, List<OpsStandardDomainLookup>> getOpsStandardMap() {
    return findAll().stream()
        .collect(Collectors.groupingBy(OpsStandardDomainLookup::getSourceCode));
  }

  /**
   * Retrieves a list of records from ops_standard_domain_lookup view in OMOP CDM based on a
   * specific source_code (OPS).
   *
   * @param sourceCode OPS code
   * @return list of records from ops_standard_domain_lookup view in OMOP CDM based on a specific
   *     source_code (OPS)
   */
  List<OpsStandardDomainLookup> findBySourceCode(String sourceCode);

  /**
   * Formats the list of records from ops_standard_domain_lookup view in OMOP CDM based on a
   * specific source_code (OPS) as a map. The source_code (OPS) is used as key.
   *
   * @param sourceCode OPS code
   * @return a map with all records from ops_standard_domain_lookup view in OMOP CDM based on a
   *     specific source_code (OPS)
   */
  default Map<String, List<OpsStandardDomainLookup>> getOpsStandardMapBySourceCode(
      String sourceCode) {
    return findBySourceCode(sourceCode).stream()
        .collect(Collectors.groupingBy(OpsStandardDomainLookup::getSourceCode));
  }
}
