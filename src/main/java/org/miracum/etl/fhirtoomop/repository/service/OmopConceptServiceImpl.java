package org.miracum.etl.fhirtoomop.repository.service;

import java.util.List;
import java.util.Map;
import org.miracum.etl.fhirtoomop.model.IcdSnomedDomainLookup;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.miracum.etl.fhirtoomop.model.SnomedRaceStandardLookup;
import org.miracum.etl.fhirtoomop.model.SnomedVaccineStandardLookup;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.repository.ConceptRepository;
import org.miracum.etl.fhirtoomop.repository.IcdSnomedRepository;
import org.miracum.etl.fhirtoomop.repository.MedicationIdRepository;
import org.miracum.etl.fhirtoomop.repository.SnomedRaceRepository;
import org.miracum.etl.fhirtoomop.repository.SnomedVaccineRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Transactional(transactionManager = "transactionManager")
@Service("OmopConceptServiceImpl")
@CacheConfig(cacheManager = "caffeineCacheManager")
public class OmopConceptServiceImpl {
  @Autowired CaffeineCacheManager cacheManager;
  @Autowired private ConceptRepository conceptRepository;
  @Autowired private MedicationIdRepository medicationIdRepository;
  @Autowired private IcdSnomedRepository icdSnomedRepository;
  @Autowired private SnomedVaccineRepository snomedVaccineRepository;
  @Autowired private SnomedRaceRepository snomedRaceRepository;
  /**
   * Returns a map of all concepts based on a specific vocabulary and concept_code.
   *
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     concept has been adapted
   * @param conceptCode value of the concept in the source vocabulary
   * @return map of all concepts based on a specific vocabulary and source value
   */
  @Cacheable(cacheNames = "valid-concepts", sync = true)
  public Map<String, List<Concept>> findValidConceptIdFromConceptCode(
      String vocabularyId, String conceptCode) {
    return conceptRepository.getConceptByVocabularyAndCode(vocabularyId, conceptCode);
  }
  /**
   * Returns a map of all ICD-to-SNOMED mappings based on a specific ICD code.
   *
   * @param icdGmCode an ICD-10-GM code
   * @return map of all ICD-to-SNOMED mappings based on a specific ICD code
   */
  @Cacheable(cacheNames = "icd-snomed", sync = true)
  public Map<String, List<IcdSnomedDomainLookup>> getIcdSnomedMap(String icdGmCode) {
    return icdSnomedRepository.getIcdSnomedMapByIcdCode(icdGmCode);
  }

  /**
   * Returns a map of all concept ids based on a specific concept name.
   *
   * @param conceptName concept name for which a concept id is searched
   * @param vocabularyId foreign key to the vocabulary table indicating from which source the
   *     concept has been adapted
   * @return map of all concept ids based on a specific concept name
   */
  @Cacheable(cacheNames = "domains", sync = true)
  public Map<String, Integer> findValidConceptIdFromConceptName(
      String conceptName, String vocabularyId) {
    return conceptRepository.findConceptByConceptName(conceptName, vocabularyId);
  }

  @Cacheable(cacheNames = "snomed-vaccine", sync = true)
  public Map<String, List<SnomedVaccineStandardLookup>> getSnomedVaccineMap(String snomedCode) {
    return snomedVaccineRepository.getSnomedVaccineMapBySnomedCode(snomedCode);
  }

  @Cacheable(cacheNames = "snomed-race", sync = true)
  public Map<String, SnomedRaceStandardLookup> getSnomedRaceMap(String snomedCode) {
    return snomedRaceRepository.getSnomedRaceMapBySnomedCode(snomedCode);
  }

  /**
   * Writes the medicationIdMap immediately to medication_id_map table in OMOP CDM.
   *
   * @param medicationIdMap the medicationIdMap to be written to OMOP CDM
   * @return medicationIdMap written to OMOP CDM
   */
  public MedicationIdMap saveAndFlush(MedicationIdMap medicationIdMap) {
    return medicationIdRepository.saveAndFlush(medicationIdMap);
  }
}
