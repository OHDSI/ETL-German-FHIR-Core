package org.miracum.etl.fhirtoomop.mapper.helpers;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_DRUG;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_PROCEDURE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_DIAGNOSTIC_REPORT_CATEGORY;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_ECRF_PARAMETER;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_GENDER;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_PROCEDURE_BODYSITE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_PROCEDURE_DICOM;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ROUTE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_SOFA_CATEGORY;
import static org.miracum.etl.fhirtoomop.Constants.STAR_CROSS_CODING_REGEX;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ATC;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ICD10GM;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_IPRD;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_LOINC;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_OPS;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ORPHA;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_UCUM;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_WHO;

import com.google.common.base.Strings;
import java.time.LocalDate;
import java.time.Year;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.model.AtcStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.IcdSnomedDomainLookup;
import org.miracum.etl.fhirtoomop.model.StandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.OpsStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.OrphaSnomedMapping;
import org.miracum.etl.fhirtoomop.model.SnomedRaceStandardLookup;
import org.miracum.etl.fhirtoomop.model.SnomedVaccineStandardLookup;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * The FindOmopConcepts class is used to find OMOP concept for FHIR code
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class FindOmopConcepts {

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;

  /**
   * Search for OMOP concept in CONCEPT table for FHIR code
   *
   * @param fhirCoding Coding element from FHIR resource
   * @param resourceDate the date from FHIR resource
   * @param bulkLoad parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @return a OMOP CONCEPT model
   */
  public Concept getConcepts(
      Coding fhirCoding,
      @Nullable LocalDate resourceDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirId) {
    if (fhirCoding == null || fhirCoding.isEmpty()) {
      return null;
    }
    var fhirCode = fhirCoding.getCode();
    if (fhirCode == null) {
      return null;
    }

    var vocabularyId = getOmopVocabularyId(fhirCoding.getSystem());
    var fhirCodeVersion = getCodeVersion(fhirCoding);
    var codeValidDate = resourceDate == null ? null : getValidDate(fhirCodeVersion, resourceDate);

    if (bulkLoad.equals(Boolean.FALSE)
        && fhirCoding.getSystem().equals("http://no-medication-code-found")) {
      return defaultMedicationConcept(fhirCode);
    }

    if (vocabularyId == null) {
      return null;
    }

    Map<String, List<Concept>> allConcepts;
    if (bulkLoad.equals(Boolean.TRUE)) {
      allConcepts = dbMappings.getOmopConceptMapWrapper().getValidConcepts(vocabularyId);
    } else {
      allConcepts = omopConceptService.findValidConceptIdFromConceptCode(vocabularyId, fhirCode);
    }

    if (!allConcepts.containsKey(fhirCode)) {
      log.info("Code [{}] of {} is not mapped in OMOP. Set concept id to 0.", fhirCode, fhirId);
      return defaultConcept(fhirCode, vocabularyId);
    }

    var omopConcepts = allConcepts.get(fhirCode);

    if (codeValidDate == null) {
      return omopConcepts.get(0);
    } else {
      for (var concept : omopConcepts) {
        if (!concept.getValidStartDate().isAfter(codeValidDate)
            && !concept.getValidEndDate().isBefore(codeValidDate)) {
          return concept;
        }
      }
    }

    log.info("Code [{}] of {} is not valid in OMOP. Skip resource.", fhirCode, fhirId);
    return null;
  }

  /**
   * Search for OMOP concept in SOURCE_TO_CONCEPT_MAP table for FHIR code
   *
   * @param fhirCode Coding element from FHIR resource
   * @param sourceVocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @return a OMOP SOURCE_TO_CONCEPT_MAP model
   */
  public SourceToConceptMap getCustomConcepts(
      String fhirCode, String sourceVocabularyId, DbMappings dbMappings) {

    var sourceToConceptMap = dbMappings.getFindHardCodeConcept();

    var omopCustomConcepts = sourceToConceptMap.get(sourceVocabularyId);
    for (var omopCustomConcept : omopCustomConcepts) {
      var sourceCode = omopCustomConcept.getSourceCode();
      if (sourceCode.equals(fhirCode)) {
        return omopCustomConcept;
      }
    }

    return defaultSourceToConceptMap(fhirCode, sourceVocabularyId);
  }

  /**
   * Search for OMOP concept in ICD_SNOMED_DOMAIN_LOOKUP view for ICD code
   *
   * @param icdCoding ICD Coding element from FHIR resource
   * @param diagnoseOnsetDate the date from FHIR resource
   * @param bulkLoad parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @return a list of ICD_SNOMED_DOMAIN_LOOKUP models
   */
  public List<IcdSnomedDomainLookup> getIcdSnomedConcepts(
      Coding icdCoding,
      LocalDate diagnoseOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirLogicId) {
    if (icdCoding.isEmpty()) {
      return Collections.emptyList();
    }

    String cleanIcdCode = icdCoding.getCode().replaceAll(STAR_CROSS_CODING_REGEX, "");
    String version = getCodeVersion(icdCoding);
    var codeValidDate = getValidDate(version, diagnoseOnsetDate);

    Map<String, List<IcdSnomedDomainLookup>> icdSnomedMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      icdSnomedMap = dbMappings.getFindIcdSnomedMapping();
    } else {
      icdSnomedMap = omopConceptService.getIcdSnomedMap(cleanIcdCode);
    }

    for (var entry : icdSnomedMap.entrySet()) {
      var key = entry.getKey();
      if (key.equalsIgnoreCase(cleanIcdCode)) {
        return entry.getValue().stream()
            .filter(
                icd ->
                    !icd.getIcdGmValidStartDate().isAfter(codeValidDate)
                        && !icd.getIcdGmValidEndDate().isBefore(codeValidDate))
            .collect(Collectors.toCollection(ArrayList::new));
      }
    }
    var defaultIcdSnomedDomainLookup =
        defaultIcdSnomedDomainLookup(icdCoding, codeValidDate, bulkLoad, dbMappings, fhirLogicId);
    if (defaultIcdSnomedDomainLookup == null) {
      return Collections.emptyList();
    }
    return List.of(defaultIcdSnomedDomainLookup);
  }

  public List<SnomedVaccineStandardLookup> getSnomedVaccineConcepts(
      Coding snomedVaccineCoding,
      LocalDate vaccineOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirLogicId,
      String fhirId) {
    if (snomedVaccineCoding.isEmpty()) {
      return Collections.emptyList();
    }

    String snomedVaccineCode = snomedVaccineCoding.getCode();
    String version = getCodeVersion(snomedVaccineCoding);
    var codeValidDate = getValidDate(version, vaccineOnsetDate);

    Map<String, List<SnomedVaccineStandardLookup>> snomedVaccineMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      snomedVaccineMap = dbMappings.getFindSnomedVaccineMapping();
    } else {
      snomedVaccineMap = omopConceptService.getSnomedVaccineMap(snomedVaccineCode);
    }

    for (var entry : snomedVaccineMap.entrySet()) {
      var key = entry.getKey();
      if (key.equalsIgnoreCase(snomedVaccineCode)) {

        var validVaccine =
            entry.getValue().stream()
                .filter(
                    snomedVaccine ->
                        !snomedVaccine.getSnomedValidStartDate().isAfter(codeValidDate)
                            && !snomedVaccine.getSnomedValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));

        if (validVaccine.isEmpty()) {
          // invalid SNOMED code
          log.warn(
              "SNOMED code [{}] of {} is not valid in OMOP. Skip resource",
              snomedVaccineCode,
              fhirId);
          return Collections.emptyList();
        } else {
          return validVaccine;
        }
      }
    }
    var defaultSnomedVaccineDomainLookup =
        defaultSnomedVaccineDomainLookup(
            snomedVaccineCoding, codeValidDate, bulkLoad, dbMappings, fhirId);
    if (defaultSnomedVaccineDomainLookup == null) {
      return Collections.emptyList();
    }
    return List.of(defaultSnomedVaccineDomainLookup);
  }

  public SnomedRaceStandardLookup getSnomedRaceConcepts(
      Coding snomedRaceCoding, Boolean bulkLoad, DbMappings dbMappings, String fhirLogicId) {
    if (snomedRaceCoding.isEmpty()) {
      return null;
    }

    String snomedRaceCode = snomedRaceCoding.getCode();

    Map<String, SnomedRaceStandardLookup> snomedRaceMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      snomedRaceMap = dbMappings.getFindSnomedRaceStandardMapping();
    } else {
      snomedRaceMap = omopConceptService.getSnomedRaceMap(snomedRaceCode);
    }

    if (!snomedRaceMap.containsKey(snomedRaceCode)) {
      return defaultSnomedRaceDomainLookup(snomedRaceCoding, bulkLoad, dbMappings, fhirLogicId);
    }

    return snomedRaceMap.get(snomedRaceCode);
  }

  /**
   * Search for OMOP concept in orpha_snomed_mapping table for Orpha code
   *
   * @param orphaCoding Orpha Coding element from FHIR resource
   * @param diagnoseOnsetDate the date from FHIR resource
   * @param bulkLoad parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param conditionLogicId logical id of the Condition FHIR resource
   * @return a list of orpha_snomed_mapping models
   */
  public List<OrphaSnomedMapping> getOrphaSnomedConcepts(
      Coding orphaCoding,
      LocalDate diagnoseOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String conditionId) {
    if (orphaCoding.isEmpty()) {
      return Collections.emptyList();
    }

    String orphaCode = orphaCoding.getCode();
    if (orphaCode == null) {
      return Collections.emptyList();
    }

    String version = getCodeVersion(orphaCoding);
    var codeValidDate = getValidDate(version, diagnoseOnsetDate);

    Map<String, List<OrphaSnomedMapping>> orphaSnomedMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      orphaSnomedMap = dbMappings.getFindOrphaSnomedMapping();
    } else {
      orphaSnomedMap = omopConceptService.getOrphaSnomedMap(orphaCode);
    }

    for (var entry : orphaSnomedMap.entrySet()) {
      var key = entry.getKey();
      if (key.equalsIgnoreCase(orphaCode)) {
        var orphaSnomedMapList = entry.getValue();

        // check if Orpha code is valid
        var validOrpha =
            orphaSnomedMapList.stream()
                .filter(
                    orpha ->
                        !orpha.getOrphaValidStartDate().isAfter(codeValidDate)
                            && !orpha.getOrphaValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));
        if (validOrpha.isEmpty()) {
          // invalid Orpha
          log.warn(
              "Orpha code [{}] of {} is not valid in OMOP. Skip resource.", orphaCode, conditionId);
          return Collections.emptyList();
        }

        // check if Orpha to SNOMED mapping is valid
        var validOrphaSnomed =
            validOrpha.stream()
                .filter(
                    orpha ->
                        !orpha.getMappingValidStartDate().isAfter(codeValidDate)
                            && !orpha.getMappingValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));

        if (validOrphaSnomed.isEmpty()) {
          // invalid Orpha to SNOMED mapping
          log.info(
              "Mapping of Orpha code [{}] of {} to Standard concept id is not valid in OMOP. Set concept id to 0.",
              orphaCode,
              conditionId);
          var defaultInvalidOrphaSnomedMapping = defaultInvalidOrphaSnomedMap(validOrpha);
          return List.of(defaultInvalidOrphaSnomedMapping);
        }
        // valid Orpha to SNOMED mapping
        return validOrphaSnomed;
      }
    }
    // Orpha code not in OMOP present
    var defaultOrphaSnomedMapping = defaultOrphaSnomedMap(orphaCoding);
    return List.of(defaultOrphaSnomedMapping);
  }

  /**
   * Search for OMOP concept in OPS_STANDARD_DOMAIN_LOOKUP view for OPS code
   *
   * @param opsCoding OPS Coding element from FHIR resource
   * @param procedureDate the date from FHIR resource
   * @param bulkLoad parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param procedureId logical id of the Procedure FHIR resource
   * @return a list of OPS_STANDARD_DOMAIN_LOOKUP models
   */
  public List<OpsStandardDomainLookup> getOpsStandardConcepts(
      Coding opsCoding,
      LocalDate procedureDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String procedureId) {
    if (opsCoding.isEmpty()) {
      return Collections.emptyList();
    }

    String opsCode = opsCoding.getCode();
    if (opsCode == null) {
      return Collections.emptyList();
    }

    String version = getCodeVersion(opsCoding);
    var codeValidDate = getValidDate(version, procedureDate);

    Map<String, List<OpsStandardDomainLookup>> opsStandardMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      opsStandardMap = dbMappings.getFindOpsStandardMapping();
    } else {
      opsStandardMap = omopConceptService.getOpsStandardMap(opsCode);
    }

    for (var entry : opsStandardMap.entrySet()) {
      var key = entry.getKey();
      if (key.equalsIgnoreCase(opsCode)) {
        var opsStandardMapList = entry.getValue();

        // check if OPS code is valid
        var validOps =
            opsStandardMapList.stream()
                .filter(
                    ops ->
                        !ops.getSourceValidStartDate().isAfter(codeValidDate)
                            && !ops.getSourceValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));
        if (validOps.isEmpty()) {
          // invalid OPS
          log.warn(
              "OPS code [{}] of {} is not valid in OMOP. Skip resource.", opsCode, procedureId);
          return Collections.emptyList();
        }

        // check if OPS to Standard mapping is valid
        var validOpsStandard =
            validOps.stream()
                .filter(
                    ops ->
                        !ops.getMappingValidStartDate().isAfter(codeValidDate)
                            && !ops.getMappingValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));

        if (validOpsStandard.isEmpty()) {
          // invalid OPS to Standard mapping
          log.info(
              "Mapping of OPS code [{}] of {} to Standard concept id is not valid in OMOP. Set concept id to 0.",
              opsCode,
              procedureId);
          var defaultInvalidOpsStandardMapping =
              defaultOpsStandardDomainLookup(
                  opsCoding, codeValidDate, bulkLoad, dbMappings, procedureId);
          return List.of(defaultInvalidOpsStandardMapping);
        }
        // valid OPS to Standard mapping
        return validOpsStandard;
      }
    }
    // OPS code not present in ops_standard_domain_lookup
    var defaultOpsStandardDomainLookup =
        defaultOpsStandardDomainLookup(opsCoding, codeValidDate, bulkLoad, dbMappings, procedureId);
    if (defaultOpsStandardDomainLookup == null) {
      return Collections.emptyList();
    }
    return List.of(defaultOpsStandardDomainLookup);
  }

  /**
   * Search for OMOP concept in ATC_STANDARD_DOMAIN_LOOKUP view for ATC code
   *
   * @param atcCoding ATC Coding element from FHIR resource
   * @param medicationDate the date from FHIR resource
   * @param bulkLoad parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param medicationId logical id of the MedicationAdministration or MedicationStatement FHIR
   *     resource
   * @return a list of ATC_STANDARD_DOMAIN_LOOKUP models
   */
  public List<AtcStandardDomainLookup> getAtcStandardConcepts(
      Coding atcCoding,
      LocalDate medicationDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String medicationId) {
    if (atcCoding.isEmpty()) {
      return Collections.emptyList();
    }

    String atcCode = atcCoding.getCode();
    if (atcCode == null) {
      return Collections.emptyList();
    }

    String version = getCodeVersion(atcCoding);
    var codeValidDate = getValidDate(version, medicationDate);

    Map<String, List<AtcStandardDomainLookup>> atcStandardMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      atcStandardMap = dbMappings.getFindAtcStandardMapping();
    } else {
      atcStandardMap = omopConceptService.getAtcStandardMap(atcCode);
    }

    for (var entry : atcStandardMap.entrySet()) {
      var key = entry.getKey();
      if (key.equalsIgnoreCase(atcCode)) {
        var atcStandardMapList = entry.getValue();

        // check if ATC code is valid
        var validAtc =
            atcStandardMapList.stream()
                .filter(
                    atc ->
                        !atc.getSourceValidStartDate().isAfter(codeValidDate)
                            && !atc.getSourceValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));
        if (validAtc.isEmpty()) {
          // invalid ATC
          log.warn(
              "ATC code [{}] of {} is not valid in OMOP. Skip resource.", atcCode, medicationId);
          return Collections.emptyList();
        }

        // check if ATC to Standard mapping is valid
        var validAtcStandard =
            validAtc.stream()
                .filter(
                    atc ->
                        !atc.getMappingValidStartDate().isAfter(codeValidDate)
                            && !atc.getMappingValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));

        if (validAtcStandard.isEmpty()) {
          // invalid ATC to Standard mapping
          log.info(
              "Mapping of ATC code [{}] of {} to Standard concept id is not valid in OMOP. Set concept id to 0.",
              atcCode,
              medicationId);
          var defaultInvalidAtcStandardMapping =
              defaultAtcStandardDomainLookup(
                  atcCoding, codeValidDate, bulkLoad, dbMappings, medicationId);
          return List.of(defaultInvalidAtcStandardMapping);
        }
        // valid ATC to Standard mapping
        return validAtcStandard;
      }
    }
    // ATC code not present in atc_standard_domain_lookup
    var defaultAtcStandardDomainLookup =
        defaultAtcStandardDomainLookup(
            atcCoding, codeValidDate, bulkLoad, dbMappings, medicationId);
    if (defaultAtcStandardDomainLookup == null) {
      return Collections.emptyList();
    }
    return List.of(defaultAtcStandardDomainLookup);
  }

  /**
   * Search for OMOP concept in LOINC_STANDARD_DOMAIN_LOOKUP view for LOINC code
   *
   * @param coding LOINC Coding element from FHIR resource
   * @param observationDate the date from FHIR resource
   * @param bulkLoad parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param observationId logical id of the Observation FHIR resource
   * @return a list of LOINC_STANDARD_DOMAIN_LOOKUP models
   */
  public List<StandardDomainLookup> getStandardConcepts(
      Coding coding,
      LocalDate observationDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String observationId) {
    if (coding.isEmpty()) {
      return Collections.emptyList();
    }

    String loincCode = coding.getCode();
    if (loincCode == null) {
      return Collections.emptyList();
    }

    String version = getCodeVersion(coding);
    var codeValidDate = getValidDate(version, observationDate);

    Map<String, List<StandardDomainLookup>> loincStandardMap;

    if (bulkLoad.equals(Boolean.TRUE)) {
      loincStandardMap = dbMappings.getFindLoincStandardMapping();
    } else {
      loincStandardMap = omopConceptService.getStandardMap(loincCode);
    }

    for (var entry : loincStandardMap.entrySet()) {
      var key = entry.getKey();
      if (key.equalsIgnoreCase(loincCode)) {
        var loincStandardMapList = entry.getValue();

        // check if LOINC code is valid
        var validLoinc =
            loincStandardMapList.stream()
                .filter(
                    loinc ->
                        !loinc.getSourceValidStartDate().isAfter(codeValidDate)
                            && !loinc.getSourceValidEndDate().isBefore(codeValidDate))
                // if required: check only for invalid_reason is not "U"
                .collect(Collectors.toCollection(ArrayList::new));
        if (validLoinc.isEmpty()) {
          // if required: check if invalid_reason = "U"
          // false -> Skip resource
          // true -> Check mapping validity

          // invalid LOINC
          log.warn(
              "LOINC code [{}] of {} is not valid in OMOP. Skip resource.",
              loincCode,
              observationId);
          return Collections.emptyList();
        }

        // check if LOINC to Standard mapping is valid
        var validLoincStandard =
            validLoinc.stream()
                .filter(
                    loinc ->
                        !loinc.getMappingValidStartDate().isAfter(codeValidDate)
                            && !loinc.getMappingValidEndDate().isBefore(codeValidDate))
                .collect(Collectors.toCollection(ArrayList::new));

        if (validLoincStandard.isEmpty()) {
          // invalid LOINC to Standard mapping
          log.info(
              "Mapping of LOINC code [{}] of {} to Standard concept id is not valid in OMOP. Set concept id to 0.",
              loincCode,
              observationId);
          var defaultInvalidLoincStandardMapping =
              defaultLoincStandardDomainLookup(
                  coding, codeValidDate, bulkLoad, dbMappings, observationId);
          return List.of(defaultInvalidLoincStandardMapping);
        }
        // valid LOINC to Standard mapping
        return validLoincStandard;
      }
    }
    // LOINC code not present in loinc_standard_domain_lookup
    var defaultLoincStandardDomainLookup =
        defaultLoincStandardDomainLookup(
            coding, codeValidDate, bulkLoad, dbMappings, observationId);
    if (defaultLoincStandardDomainLookup == null) {
      return Collections.emptyList();
    }
    return List.of(defaultLoincStandardDomainLookup);
  }

  /**
   * Extract OMOP vocabulary ID based on the used system URL in Coding
   *
   * @param terminologySystemUrl the used system URL in Coding
   * @return OMOP vocabulary ID
   */
  public String getOmopVocabularyId(String terminologySystemUrl) {

    var fhirSystemUrl = FhirSystems.fhirEnum.getFhirUrl(terminologySystemUrl);
    if (fhirSystemUrl == null) {
      return null;
    }

    switch (fhirSystemUrl) {
      case LOINC:
        return VOCABULARY_LOINC;
      case SNOMED:
        return VOCABULARY_SNOMED;
      case ATC:
        return VOCABULARY_ATC;
      case OPS:
        return VOCABULARY_OPS;
      case UCUM:
        return VOCABULARY_UCUM;
      case ICD10GM:
        return VOCABULARY_ICD10GM;
      case ORPHA:
        return VOCABULARY_ORPHA;
      case GENDERAMTLICHDEEXTENSION:
        return SOURCE_VOCABULARY_ID_GENDER;
      case PROCEDURESITELOCALIZATION:
        return SOURCE_VOCABULARY_ID_PROCEDURE_BODYSITE;
      case EDQM:
        return SOURCE_VOCABULARY_ROUTE;
      case LABOBSERVATIONCATEGORY:
        return SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY;
      case PROCEDUREDICOM:
        return SOURCE_VOCABULARY_ID_PROCEDURE_DICOM;
      case GECCOECRFPARAMETER:
        return SOURCE_VOCABULARY_ID_ECRF_PARAMETER;
      case GECCOSOFASCORE:
        return SOURCE_VOCABULARY_SOFA_CATEGORY;
      case IPRDSYSTEM:
        return VOCABULARY_IPRD;
      case WHOSYSTEM:
        return VOCABULARY_WHO;
      default:
        return null;
    }
  }

  /**
   * Extract version of Code in FHIR resource
   *
   * @param fhirCoding Coding element from FHIR resource
   * @return version of Code
   */
  private String getCodeVersion(Coding fhirCoding) {
    var versionElement = fhirCoding.getVersionElement();
    if (versionElement.isEmpty()) {
      return null;
    }
    var version = checkDataAbsentReason.getValue(versionElement);

    if (Strings.isNullOrEmpty(version)) {
      return null;
    }
    return version;
  }

  /**
   * Extract valid data of code in FHIR resource
   *
   * @param version version of Code
   * @param recordedDate the date from FHIR resource
   * @return valid Date of code in FHIR resource
   */
  private LocalDate getValidDate(String version, LocalDate recordedDate) {
    LocalDate versionDate;
    try {
      var versionYear = Year.parse(version);
      versionDate = versionYear.atMonth(12).atDay(31);
    } catch (Exception ignored) {
      return recordedDate;
    }
    if (versionDate.getYear() == recordedDate.getYear()) {
      return recordedDate;
    } else {
      return versionDate;
    }
  }

  /**
   * Default OMOP SOURCE_TO_CONCEPT_MAP model, when the code from FHIR resource is not exist in OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP SOURCE_TO_CONCEPT_MAP model
   */
  private SourceToConceptMap defaultSourceToConceptMap(String fhirCode, String vocabularyId) {
    var defaultSourceToConceptMap =
        SourceToConceptMap.builder()
            .sourceCode(fhirCode)
            .targetConceptId(CONCEPT_NO_MATCHING_CONCEPT)
            .build();
    if (SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY.equals(vocabularyId)
        || SOURCE_VOCABULARY_ID_DIAGNOSTIC_REPORT_CATEGORY.equals(vocabularyId)) {
      defaultSourceToConceptMap.setTargetConceptId(CONCEPT_EHR);
    }
    return defaultSourceToConceptMap;
  }

  /**
   * Default OMOP CONCEPT model, when the code from FHIR resource is not exist in OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP CONCEPT model
   */
  private Concept defaultConcept(String fhirCode, String vocabularyId) {
    if (Strings.isNullOrEmpty(vocabularyId)) {
      return Concept.builder()
          .conceptCode(fhirCode)
          .conceptId(CONCEPT_NO_MATCHING_CONCEPT)
          .domainId(OMOP_DOMAIN_OBSERVATION)
          .build();
    }
    var defaultConcept =
        Concept.builder().conceptCode(fhirCode).conceptId(CONCEPT_NO_MATCHING_CONCEPT).build();
    if (vocabularyId.equals(VOCABULARY_LOINC) || vocabularyId.equals(VOCABULARY_SNOMED)) {
      defaultConcept.setDomainId(OMOP_DOMAIN_OBSERVATION);
    }
    if (vocabularyId.equals(VOCABULARY_ICD10GM) || vocabularyId.equals(VOCABULARY_ORPHA)) {
      defaultConcept.setDomainId(OMOP_DOMAIN_CONDITION);
    }

    if (vocabularyId.equals(VOCABULARY_OPS)) {
      defaultConcept.setDomainId(OMOP_DOMAIN_PROCEDURE);
    }

    if (vocabularyId.equals(VOCABULARY_ATC)) {
      defaultConcept.setDomainId(OMOP_DOMAIN_DRUG);
    }

    return defaultConcept;
  }

  private Concept defaultMedicationConcept(String fhirCode) {
    return Concept.builder()
        .conceptCode(fhirCode)
        .conceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .domainId(OMOP_DOMAIN_DRUG)
        .build();
  }

  /**
   * Default OMOP ICD_SNOMED_DOMAIN_LOOKUP model, when the code from FHIR resource does not exist in
   * OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP ICD_SNOMED_DOMAIN_LOOKUP model
   */
  private IcdSnomedDomainLookup defaultIcdSnomedDomainLookup(
      Coding icdCoding,
      LocalDate diagnoseOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirId) {
    var omopConcept = getConcepts(icdCoding, diagnoseOnsetDate, bulkLoad, dbMappings, fhirId);
    if (omopConcept == null) {
      return null;
    }
    return IcdSnomedDomainLookup.builder()
        .icdGmCode(icdCoding.getCode())
        .icdGmConceptId(omopConcept.getConceptId())
        .snomedConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .snomedDomainId(omopConcept.getDomainId())
        .icdGmValidStartDate(omopConcept.getValidStartDate())
        .icdGmValidEndDate(omopConcept.getValidEndDate())
        .build();
  }

  /**
   * Default OMOP SNOMED_VACCINE_DOMAIN_LOOKUP model, when the code from FHIR resource does not
   * exist in OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP ICD_SNOMED_DOMAIN_LOOKUP model
   */
  private SnomedVaccineStandardLookup defaultSnomedVaccineDomainLookup(
      Coding snomedVaccineCoding,
      LocalDate vaccineOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirId) {
    var omopConcept =
        getConcepts(snomedVaccineCoding, vaccineOnsetDate, bulkLoad, dbMappings, fhirId);
    if (omopConcept == null) {
      return null;
    }
    return SnomedVaccineStandardLookup.builder()
        .snomedCode(snomedVaccineCoding.getCode())
        .snomedConceptId(omopConcept.getConceptId())
        .standardVaccineConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .standardVaccineDomainId(OMOP_DOMAIN_DRUG)
        .build();
  }

  /**
   * Default OMOP SNOMED_RACE_DOMAIN_LOOKUP model, when the code from FHIR resource does not exist
   * in OMOP
   *
   * @param fhirCode code from FHIR resource
   * @return the default OMOP SNOMED_RACE_DOMAIN_LOOKUP model
   */
  private SnomedRaceStandardLookup defaultSnomedRaceDomainLookup(
      Coding snomedRaceCoding, Boolean bulkLoad, DbMappings dbMappings, String fhirId) {
    var raceConcept = getConcepts(snomedRaceCoding, null, bulkLoad, dbMappings, fhirId);
    if (raceConcept == null) {
      return null;
    }
    return SnomedRaceStandardLookup.builder()
        .snomedCode(snomedRaceCoding.getCode())
        .snomedConceptId(raceConcept.getConceptId())
        .standardRaceConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .build();
  }

  /**
   * Default OMOP orpha_snomed_mapping model, when the code from FHIR resource does not exist in
   * OMOP
   *
   * @param orphaCoding Orpha code from FHIR resource
   * @return the default OMOP orpha_snomed_mapping model
   */
  private OrphaSnomedMapping defaultOrphaSnomedMap(Coding orphaCoding) {

    return OrphaSnomedMapping.builder()
        .orphaCode(orphaCoding.getCode())
        .orphaConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .snomedConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .snomedDomainId(OMOP_DOMAIN_CONDITION)
        .build();
  }

  /**
   * Default OMOP orpha_snomed_mapping model, when the mapping between Orpha and SNOMED is invalid
   *
   * @param orphaCoding Orpha code from FHIR resource
   * @return the default OMOP orpha_snomed_mapping model
   */
  private OrphaSnomedMapping defaultInvalidOrphaSnomedMap(
      List<OrphaSnomedMapping> orphaSnomedMapList) {
    var orphaSnomedMap = orphaSnomedMapList.get(0);

    return OrphaSnomedMapping.builder()
        .orphaCode(orphaSnomedMap.getOrphaCode())
        .orphaConceptId(orphaSnomedMap.getOrphaConceptId())
        .snomedConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .snomedDomainId(OMOP_DOMAIN_CONDITION)
        .build();
  }

  /**
   * Default ops_standard_domain_lookup model, when the code from FHIR resource is not present in
   * ops_standard_domain_lookup or concept or if the mapping between OPS and Standard is not valid.
   *
   * @param opsCoding OPS Coding element from FHIR resource
   * @return the default OMOP ops_standard_domain_lookup model
   */
  private OpsStandardDomainLookup defaultOpsStandardDomainLookup(
      Coding opsCoding,
      LocalDate procedureDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirId) {
    var omopConcept = getConcepts(opsCoding, procedureDate, bulkLoad, dbMappings, fhirId);
    if (omopConcept == null) {
      return null;
    }
    return OpsStandardDomainLookup.builder()
        .sourceCode(opsCoding.getCode())
        .sourceConceptId(omopConcept.getConceptId())
        .standardConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .standardDomainId(omopConcept.getDomainId())
        .build();
  }

  /**
   * Default atc_standard_domain_lookup model, when the code from FHIR resource is not present in
   * atc_standard_domain_lookup or concept or if the mapping between ATC and Standard is not valid.
   *
   * @param atcCoding ATC Coding element from FHIR resource
   * @return the default OMOP atc_standard_domain_lookup model
   */
  private AtcStandardDomainLookup defaultAtcStandardDomainLookup(
      Coding atcCoding,
      LocalDate medicationDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirId) {
    var omopConcept = getConcepts(atcCoding, medicationDate, bulkLoad, dbMappings, fhirId);
    if (omopConcept == null) {
      return null;
    }
    return AtcStandardDomainLookup.builder()
        .sourceCode(atcCoding.getCode())
        .sourceConceptId(omopConcept.getConceptId())
        .standardConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .standardDomainId(omopConcept.getDomainId())
        .build();
  }

  /**
   * Default loinc_standard_domain_lookup model, when the code from FHIR resource is not present in
   * loinc_standard_domain_lookup or concept or if the mapping between LOINC and Standard is not
   * valid.
   *
   * @param loincCoding LOINC Coding element from FHIR resource
   * @return the default OMOP loinc_standard_domain_lookup model
   */
  private StandardDomainLookup defaultLoincStandardDomainLookup(
      Coding loincCoding,
      LocalDate observationDate,
      Boolean bulkLoad,
      DbMappings dbMappings,
      String fhirId) {
    var omopConcept = getConcepts(loincCoding, observationDate, bulkLoad, dbMappings, fhirId);
    if (omopConcept == null) {
      return null;
    }
    return StandardDomainLookup.builder()
        .sourceCode(loincCoding.getCode())
        .sourceConceptId(omopConcept.getConceptId())
        .standardConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .standardDomainId(omopConcept.getDomainId())
        .build();
  }
}
