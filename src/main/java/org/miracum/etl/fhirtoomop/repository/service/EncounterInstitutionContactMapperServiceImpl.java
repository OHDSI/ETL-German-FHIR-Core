package org.miracum.etl.fhirtoomop.repository.service;

import java.util.List;
import org.miracum.etl.fhirtoomop.repository.FactRelationshipRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.miracum.etl.fhirtoomop.repository.PostProcessMapRepository;
import org.miracum.etl.fhirtoomop.repository.VisitOccRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The EncounterInstitutionContactMapperServiceImpl class contains the specific implementation to
 * access data trough OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional(transactionManager = "transactionManager")
@Service("EncounterInstitutionContactMapperServiceImpl")
@CacheConfig(cacheManager = "caffeineCacheManager")
public class EncounterInstitutionContactMapperServiceImpl {
  @Autowired CaffeineCacheManager cacheManager;
  @Autowired VisitOccRepository visitOccRepository;
  @Autowired PostProcessMapRepository ppmRepository;
  @Autowired FactRelationshipRepository frRepository;
  @Autowired ObservationRepository obsRepository;

  /**
   * Searches if a FHIR Encounter resource already exists in visit_occurrence table in OMOP CDM
   * based on the logical id of the FHIR Encounter resource.
   *
   * @param logicalId logical id of the Encounter resource
   * @return the existing visit_occurrence_id
   */
  @Cacheable(cacheNames = "visits-logicalid", sync = true)
  public Long findVisitsOccIdByFhirLogicalId(String logicalId) {
    var existingVisitOcc = visitOccRepository.findByFhirLogicalId(logicalId);
    if (existingVisitOcc == null) {
      return null;
    }
    return existingVisitOcc.getVisitOccurrenceId();
  }

  /**
   * Searches if a FHIR Encounter resource already exists in visit_occurrence table in OMOP CDM
   * based on the identifier of the FHIR Encounter resource.
   *
   * @param identifier identifier of the Encounter resource
   * @return the existing visit_occurrence_id
   */
  @Cacheable(cacheNames = "visits-identifier", sync = true)
  public Long findVisitsOccIdByFhirIdentifier(String identifier) {
    var existingVisitOcc = visitOccRepository.findByFhirIdentifier(identifier);
    if (existingVisitOcc == null) {
      return null;
    }
    return existingVisitOcc.getVisitOccurrenceId();
  }

  /**
   * Delete FHIR Encounter resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Encounter resource
   */
  public void deleteVisitOccByLogicalId(String fhirLogicalId) {
    visitOccRepository.deleteByFhirLogicalId(fhirLogicalId);
    obsRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR Encounter resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Encounter resource
   */
  public void deleteVisitOccByIdentifier(String fhirIdentifier) {
    visitOccRepository.deleteByFhirIdentifier(fhirIdentifier);
    obsRepository.deleteByFhirIdentifier(fhirIdentifier);
  }

  /**
   * Deletes diagnosis information from post_process_map using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Encounter resource
   */
  public void deleteExistingEncounterByFhirLogicalId(String fhirLogicalId) {
    ppmRepository.deleteEncounterByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Deletes diagnosis information from post_process_map using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Encounter resource
   */
  public void deleteExistingEncounterByFhirIdentifier(String fhirIdentifier) {
    ppmRepository.deleteEncounterByFhirIdentifier(fhirIdentifier);
  }

  /**
   * Deletes primary secondary ICD information from fact_relationship and updates flag for primary
   * secondary ICD information in post_process_map using fhir_logical_ids of the referenced FHIR
   * Condition resources
   *
   * @param diagnosisReferenceList list of logical ids of the referenced FHIR Condition resources
   */
  public void updatePrimarySecondaryInformation(List<String> diagnosisReferenceList) {
    frRepository.deletePrimarySecondaryByFhirLogicalId(diagnosisReferenceList);
    ppmRepository.updatePrimarySecondaryByFhirLogicalId(diagnosisReferenceList);
  }
}
