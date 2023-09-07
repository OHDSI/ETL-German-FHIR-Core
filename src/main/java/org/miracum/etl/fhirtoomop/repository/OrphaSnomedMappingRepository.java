package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.transaction.Transactional;
import org.miracum.etl.fhirtoomop.model.OrphaSnomedMapping;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The OrphaSnomedMappingRepository interface represents a repository for the orpha_snomed_mapping
 * table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface OrphaSnomedMappingRepository
    extends PagingAndSortingRepository<OrphaSnomedMapping, Long>,
        JpaRepository<OrphaSnomedMapping, Long> {
  /** Empties the orpha_snomed_mapping table in OMOP CDM. */
  @Transactional
  @Modifying
  @Query(value = "TRUNCATE TABLE cds_etl_helper.orpha_snomed_mapping", nativeQuery = true)
  void truncateTable();

  /**
   * Retrieves a list of all records from orpha_snomed_mapping table in OMOP CDM.
   *
   * @return list of all records from orpha_snomed_mapping table in OMOP CDM
   */
  @Override
  List<OrphaSnomedMapping> findAll();

  /**
   * Formats the list of all records from orpha_snomed_mapping table in OMOP CDM as a map. The orpha
   * is used as key.
   *
   * @return a map with all records from orpha_snomed_mapping table using Orpha code as key
   */
  default Map<String, List<OrphaSnomedMapping>> getOrphaSnomedMap() {
    return findAll().stream().collect(Collectors.groupingBy(OrphaSnomedMapping::getOrphaCode));
  }

  /**
   * Retrieves a list of records from orpha_snomed_mapping table in OMOP CDM based on a specific
   * Orpha code.
   *
   * @param orphaCode Orpha code
   * @return list of records from orpha_snomed_mapping table in OMOP CDM based on a specific Orpha
   *     code
   */
  List<OrphaSnomedMapping> findByOrphaCode(String orphaCode);

  /**
   * Formats the list of records from orpha_snomed_mapping table in OMOP CDM based on a specific
   * Orpha code as a map. The Orpha code is used as key.
   *
   * @param orphaCode Orpha code
   * @return a map with all records from orpha_snomed_mapping table in OMOP CDM based on a specific
   *     Orpha code
   */
  default Map<String, List<OrphaSnomedMapping>> getOrphaSnomedMapByOrphaCode(String orphaCode) {
    return findByOrphaCode(orphaCode).stream()
        .collect(Collectors.groupingBy(OrphaSnomedMapping::getOrphaCode));
  }
}
