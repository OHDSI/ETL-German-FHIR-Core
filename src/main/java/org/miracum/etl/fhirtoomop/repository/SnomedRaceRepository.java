package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.SnomedRaceStandardLookup;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The IcdSnomedRepository interface represents a repository for the icd_snomed_domain_lookup view
 * in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface SnomedRaceRepository
    extends PagingAndSortingRepository<SnomedRaceStandardLookup, Long> {

  /**
   * Retrieves a list of all records from icd_snomed_domain_lookup view in OMOP CDM.
   *
   * @return list of all records from icd_snomed_domain_lookup view in OMOP CDM
   */
  @Override
  List<SnomedRaceStandardLookup> findAll();

  /**
   * Formats the list of all records from icd_snomed_domain_lookup view in OMOP CDM as a map. The
   * icd_gm_code is used as key.
   *
   * @return a map with all records from icd_snomed_domain_lookup view using icd_gm_code as key
   */
  default Map<String, SnomedRaceStandardLookup> getSnomedRaceMap() {
    return findAll().stream()
        .collect(Collectors.toMap(SnomedRaceStandardLookup::getSnomedCode, v -> v));
  }

  /**
   * Retrieves a list of records from icd_snomed_domain_lookup view in OMOP CDM based on a specific
   * icd_gm_code.
   *
   * @param icdGmCode ICD-10-GM code
   * @return list of records from icd_snomed_domain_lookup view in OMOP CDM based on a specific
   *     icd_gm_code
   */
  List<SnomedRaceStandardLookup> findBySnomedCode(String snomedCode);

  /**
   * Formats the list of records from icd_snomed_domain_lookup view in OMOP CDM based on a specific
   * icd_gm_code as a map. The icd_gm_code is used as key.
   *
   * @param icdGmCode ICD-10-GM code
   * @return a map with all records from icd_snomed_domain_lookup view in OMOP CDM based on a
   *     specific icd_gm_code
   */
  default Map<String, SnomedRaceStandardLookup> getSnomedRaceMapBySnomedCode(String snomedCode) {
    return findBySnomedCode(snomedCode).stream()
        .collect(Collectors.toMap(SnomedRaceStandardLookup::getSnomedCode, v -> v));
  }
}
