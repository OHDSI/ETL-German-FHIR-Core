package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.omop.CareSite;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The CareSiteRepository interface represents a repository for the care_site table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface CareSiteRepository extends PagingAndSortingRepository<CareSite, Long> {

  /**
   * Retrieves a list of all records from care_site table in OMOP CDM.
   *
   * @return list of all records from care_site table in OMOP CDM
   */
  @Override
  List<CareSite> findAll();

  /**
   * Formats the list of all records from care_site table in OMOP CDM as a map. The
   * care_site_source_value is used as key.
   *
   * @return a map with all records from care_site table using care_site_source_value as key
   */
  default Map<String, CareSite> careSitesMap() {
    return findAll().stream().collect(Collectors.toMap(CareSite::getCareSiteSourceValue, v -> v));
  }
}
