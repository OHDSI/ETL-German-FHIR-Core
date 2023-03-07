package org.miracum.etl.fhirtoomop.mapper.helpers;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_DRUG;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
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
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_LOINC;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_OPS;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_UCUM;

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
import org.miracum.etl.fhirtoomop.model.IcdSnomedDomainLookup;
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
  @Autowired Boolean dictionaryLoadInRam;

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
      DbMappings dbMappings) {
    if (fhirCoding == null || fhirCoding.isEmpty()) {
      return null;
    }
    var fhirCode = fhirCoding.getCode();
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
    if (bulkLoad.equals(Boolean.TRUE) && dictionaryLoadInRam.equals(Boolean.TRUE)) {
      allConcepts = dbMappings.getOmopConceptMapWrapper().getValidConcepts(vocabularyId);
    } else {
      allConcepts = omopConceptService.findValidConceptIdFromConceptCode(vocabularyId, fhirCode);
    }

    if (!allConcepts.containsKey(fhirCode)) {
      log.info("Code [{}] is not mapped in OMOP. Set concept id to 0.", fhirCode);
      return defaultConcept(fhirCode, vocabularyId);
    }

    var omopConcepts = allConcepts.get(fhirCode);

    if (codeValidDate == null) {
      return omopConcepts.get(0);
    }
    for (var concept : omopConcepts) {
      if (!concept.getValidStartDate().isAfter(codeValidDate)
          && !concept.getValidEndDate().isBefore(codeValidDate)) {
        return concept;
      }
    }

    log.info("Code [{}] is not valid in OMOP.", fhirCode);
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
      Coding fhirCoding, String sourceVocabularyId, DbMappings dbMappings) {
    if (fhirCoding == null) {
      return null;
    }
    var fhirCode = fhirCoding.getCode();
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
      Coding icdCoding, LocalDate diagnoseOnsetDate, Boolean bulkLoad, DbMappings dbMappings) {
    if (icdCoding.isEmpty()) {
      return Collections.emptyList();
    }

    String cleanIcdCode = icdCoding.getCode().replaceAll(STAR_CROSS_CODING_REGEX, "");
    String version = getCodeVersion(icdCoding);
    var codeValidDate = getValidDate(version, diagnoseOnsetDate);

    Map<String, List<IcdSnomedDomainLookup>> icdSnomedMap;

    if (bulkLoad.equals(Boolean.TRUE) && dictionaryLoadInRam.equals(Boolean.TRUE)) {
      icdSnomedMap = dbMappings.getFindIcdSnomedMapping();
    } else {
      icdSnomedMap = omopConceptService.getIcdSnomedMap(cleanIcdCode);
    }

    var keySet = icdSnomedMap.keySet();
    if (!keySet.contains(cleanIcdCode)) {
      var defaultIcdSnomedDomainLookup =
          defaultIcdSnomedDomainLookup(icdCoding, codeValidDate, bulkLoad, dbMappings);
      if (defaultIcdSnomedDomainLookup == null) {
        return Collections.emptyList();
      }
      return List.of(defaultIcdSnomedDomainLookup);
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
    return Collections.emptyList();
  }

  public List<SnomedVaccineStandardLookup> getSnomedVaccineConcepts(
      Coding snomedVaccineCoding,
      LocalDate vaccineOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings) {
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
        return entry.getValue().stream()
            .filter(
                snomedVaccine ->
                    !snomedVaccine.getSnomedValidStartDate().isAfter(codeValidDate)
                        && !snomedVaccine.getSnomedValidEndDate().isBefore(codeValidDate))
            .collect(Collectors.toCollection(ArrayList::new));
      }
    }
    var defaultSnomedVaccineDomainLookup =
        defaultSnomedVaccineDomainLookup(snomedVaccineCoding, codeValidDate, bulkLoad, dbMappings);
    if (defaultSnomedVaccineDomainLookup == null) {
      return Collections.emptyList();
    }
    return List.of(defaultSnomedVaccineDomainLookup);
  }

  public SnomedRaceStandardLookup getSnomedRaceConcepts(
      Coding snomedRaceCoding, Boolean bulkLoad, DbMappings dbMappings) {
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
      return defaultSnomedRaceDomainLookup(snomedRaceCoding, bulkLoad, dbMappings);
    }

    return snomedRaceMap.get(snomedRaceCode);
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
    var opsVersion = checkDataAbsentReason.getValue(versionElement);

    if (Strings.isNullOrEmpty(opsVersion)) {
      return null;
    }
    return opsVersion;
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
   * Default OMOP SOURCE_TO_CONCEPT_MAP model, when the code from FHIR resource is not exits in OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP SOURCE_TO_CONCEPT_MAP model
   */
  private SourceToConceptMap defaultSourceToConceptMap(String fhirCode, String vocabularyId) {

    if (!Strings.isNullOrEmpty(vocabularyId)) {
      var defaultSourceToConceptMap = SourceToConceptMap.builder().sourceCode(fhirCode).build();
      if (vocabularyId.equals(SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY)
          || vocabularyId.equals(SOURCE_VOCABULARY_ID_DIAGNOSTIC_REPORT_CATEGORY)) {
        defaultSourceToConceptMap.setTargetConceptId(CONCEPT_EHR);
      }
      return defaultSourceToConceptMap;
    } else {
      return SourceToConceptMap.builder()
          .sourceCode(fhirCode)
          .targetConceptId(CONCEPT_NO_MATCHING_CONCEPT)
          .build();
    }
  }

  /**
   * Default OMOP CONCEPT model, when the code from FHIR resource is not exits in OMOP
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
    if (vocabularyId.equals(VOCABULARY_ICD10GM)) {
      defaultConcept.setDomainId(OMOP_DOMAIN_CONDITION);
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
   * Default OMOP ICD_SNOMED_DOMAIN_LOOKUP model, when the code from FHIR resource is not exits in
   * OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP ICD_SNOMED_DOMAIN_LOOKUP model
   */
  private IcdSnomedDomainLookup defaultIcdSnomedDomainLookup(
      Coding icdCoding, LocalDate diagnoseOnsetDate, Boolean bulkLoad, DbMappings dbMappings) {
    var omopConcept = getConcepts(icdCoding, diagnoseOnsetDate, bulkLoad, dbMappings);
    if (omopConcept == null) {
      return null;
    }
    return IcdSnomedDomainLookup.builder()
        .icdGmCode(icdCoding.getCode())
        .icdGmConceptId(omopConcept.getConceptId())
        .snomedConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        //        .snomedDomainId(OMOP_DOMAIN_CONDITION)
        .snomedDomainId(omopConcept.getDomainId())
        .icdGmValidStartDate(omopConcept.getValidStartDate())
        .icdGmValidEndDate(omopConcept.getValidEndDate())
        .build();
  }

  /**
   * Default OMOP SNOMED_VACCINE_DOMAIN_LOOKUP model, when the code from FHIR resource is not exits
   * in OMOP
   *
   * @param fhirCode code from FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return the default OMOP ICD_SNOMED_DOMAIN_LOOKUP model
   */
  private SnomedVaccineStandardLookup defaultSnomedVaccineDomainLookup(
      Coding snomedVaccineCoding,
      LocalDate vaccineOnsetDate,
      Boolean bulkLoad,
      DbMappings dbMappings) {
    var omopConcept = getConcepts(snomedVaccineCoding, vaccineOnsetDate, bulkLoad, dbMappings);
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
   * Default OMOP SNOMED_RACE_DOMAIN_LOOKUP model, when the code from FHIR resource is not exits in
   * OMOP
   *
   * @param fhirCode code from FHIR resource
   * @return the default OMOP SNOMED_RACE_DOMAIN_LOOKUP model
   */
  private SnomedRaceStandardLookup defaultSnomedRaceDomainLookup(
      Coding snomedRaceCoding, Boolean bulkLoad, DbMappings dbMappings) {
    var raceConcept = getConcepts(snomedRaceCoding, null, bulkLoad, dbMappings);
    if (raceConcept == null) {
      return null;
    }
    return SnomedRaceStandardLookup.builder()
        .snomedCode(snomedRaceCoding.getCode())
        .snomedConceptId(raceConcept.getConceptId())
        .standardRaceConceptId(CONCEPT_NO_MATCHING_CONCEPT)
        .build();
  }
}
