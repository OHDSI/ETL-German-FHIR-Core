package org.miracum.etl.fhirtoomop.repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.transaction.annotation.Transactional;

/**
 * The MedicationIdRepository interface represents a repository for the medication_id_map table in
 * OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface MedicationIdRepository
    extends PagingAndSortingRepository<MedicationIdMap, Long>,
        JpaRepository<MedicationIdMap, Long> {

  /** Empties the medication_id_map table in OMOP CDM. */
  @Transactional
  @Modifying
  @Query(value = "TRUNCATE TABLE cds_etl_helper.medication_id_map", nativeQuery = true)
  void truncateTable();

  /**
   * Retrieves a list of all records from medication_id_map table in OMOP CDM.
   *
   * @return list of all records from medication_id_map table in OMOP CDM
   */
  @Override
  List<MedicationIdMap> findAll();

  /**
   * Formats the list of all records from medication_id_map table in OMOP CDM as a map. The atc is
   * used as key.
   *
   * @return a map with all records from medication_id_map table using atc as key
   */
  default Map<String, List<MedicationIdMap>> getMedications() {
    return findAll().stream().collect(Collectors.groupingBy(MedicationIdMap::getAtc));
  }

  /**
   * Retrieves a list of all records from medication_id_map table in OMOP CDM based on a specific
   * fhir_identifier.
   *
   * @param fhirIdentifier identifier for the source data in the FHIR resource
   * @return list of all records from medication_id_map table in OMOP CDM based on a specific
   *     fhir_identifier
   */
  List<MedicationIdMap> findByFhirIdentifier(String fhirIdentifier);

  /**
   * Retrieves a list of all records from medication_id_map table in OMOP CDM based on a specific
   * fhir_logical_id.
   *
   * @param fhirLogicalId logical id of the FHIR resource
   * @return list of all records from medication_id_map table in OMOP CDM based on a specific
   *     fhir_logical_id
   */
  List<MedicationIdMap> findByFhirLogicalId(String fhirLogicalId);

  /**
   * Retrieves the maximum value of fhir_omop_id from medication_id_map table in OMOP CDM.
   *
   * @return the maximum value of fhir_omop_id
   */
  @Query(
      value = "SELECT MAX(fhir_omop_id) FROM cds_etl_helper.medication_id_map",
      nativeQuery = true)
  Long getMaxMedicationId();

  //  @Query(
  //      "SELECT map FROM MedicationIdMap map WHERE "
  //          + "(map.fhirLogicalId=:fhirLogicalId OR map.fhirIdentifier=:fhirIdentifier)")
  //  MedicationIdMap getFhirOmopIdByResource(
  //      @Param("fhirLogicalId") String fhirLogicalId, @Param("fhirIdentifier") String
  // fhirIdentifier);
  //
  //  Page<MedicationIdMap> findByType(String type, Pageable pageable);
  //
  //  default Map<String, List<MedicationIdMap>> getMedicationIdMapByType(
  //      String type, Pageable pageable) {
  //    return findByType(type, pageable).getContent().stream()
  //        .collect(Collectors.groupingBy(MedicationIdMap::getAtc));
  //  }
}
