package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_SEVERITY;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_STAGE;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_MEASUREMENT;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_PROCEDURE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_DIAGNOSTIC_CONFIDENCE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_ICD_LOCALIZATION;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Extension;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.IcdSnomedDomainLookup;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.ConditionOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.Measurement;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.model.omop.ProcedureOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.repository.service.ConditionMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The ConditionMapper class describes the business logic of transforming a FHIR Condition resource
 * to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class ConditionMapper implements FhirMapper<Condition> {

  private static final FhirSystems fhirSystems = new FhirSystems();
  private final DbMappings dbMappings;
  private final Boolean bulkload;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ConditionMapperServiceImpl conditionService;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessConditions");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessConditions");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessConditions");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessConditions");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessConditions");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessConditions");

  /**
   * Constructor for objects of the class ConditionMapper.
   *
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public ConditionMapper(Boolean bulkload, DbMappings dbMappings) {

    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR Condition resource to several OMOP CDM tables.
   *
   * @param srcCondition FHIR Condition resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Condition
   *     resource
   */
  @Override
  public OmopModelWrapper map(Condition srcCondition, boolean isDeleted) {
    var wrapper = new OmopModelWrapper();

    var conditionLogicId = fhirReferenceUtils.extractId(srcCondition);

    var conditionSourceIdentifier = fhirReferenceUtils.extractResourceFirstIdentifier(srcCondition);
    if (Strings.isNullOrEmpty(conditionLogicId)
        && Strings.isNullOrEmpty(conditionSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Condition] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }
    var clinicalStatusValue = getClinicalStatus(srcCondition);
    var verificationStatusValue = getVerificationStatusValue(srcCondition);

    //    if (Strings.isNullOrEmpty(statusValue)
    //        || !FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST.contains(statusValue)) {
    if (!diagnoseStatusProvement(clinicalStatusValue, verificationStatusValue)) {
      log.error(
          "The combination of [clinical status]: {} and [verification status]: {} of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          clinicalStatusValue == null ? null : clinicalStatusValue.getCode(),
          verificationStatusValue == null ? null : verificationStatusValue.getCode(),
          conditionLogicId);
      return null;
    String conditionId = "";
    if (!Strings.isNullOrEmpty(conditionLogicId)) {
      conditionId = srcCondition.getId();
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingConditionEntry(conditionLogicId, conditionSourceIdentifier);
      if (isDeleted) {
        deleteExistingPostProcessMapEntry(conditionLogicId, conditionSourceIdentifier);
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", conditionLogicId);
        return null;
      }
      updateExistingPostProcessMapEntry(conditionLogicId, conditionSourceIdentifier);
    }

    var uncheckedDiagnoseCodingList = getDiagnoseCoding(srcCondition);
    if (uncheckedDiagnoseCodingList.isEmpty()) {
      log.warn("No Diagnose Code found for [Condition]: {}. Skip resource.", conditionLogicId);
      noCodeCounter.increment();
      return null;
    }

    var personId = getPersonId(srcCondition, conditionLogicId, conditionId);
    if (personId == null) {
      log.warn("No matching [Person] found for [Condition]: {}. Skip resource", conditionLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcCondition, conditionId, personId);

    var diagnoseOnset = getConditionOnset(srcCondition);
    if (diagnoseOnset.getStartDateTime() == null) {
      log.warn("No [Date] found for [Condition]: {}. Skip resource", conditionLogicId);
      noStartDateCounter.increment();
      return null;
    }

    //    List<Coding> uncheckedDiagnoseCodings = new ArrayList<>();
    //    for (var diagnoseCoding : diagnoseCodingList) {
    //      var uncheckedDiagnoseCoding = splitDiagnoseCodes(diagnoseCoding, null);
    //      if (uncheckedDiagnoseCoding.isEmpty() || uncheckedDiagnoseCoding == null) {
    //        log.warn("No Diagnose Code found for [Condition]: {}. Skip resource.",
    // conditionLogicId);
    //        noCodeCounter.increment();
    //        return null;
    //      }
    //      uncheckedDiagnoseCodings.addAll(uncheckedDiagnoseCoding);
    //    }

    var validIcdSnomedConceptMaps =
        getValidIcdSnomedCodes(
            uncheckedDiagnoseCodingList,
            diagnoseOnset.getStartDateTime().toLocalDate(),
            conditionLogicId);

    if (validIcdSnomedConceptMaps.isEmpty()) {
      log.warn("No Diagnose Code in [Condition]: {} is invalid. Skip resource.", conditionLogicId);
      noCodeCounter.increment();
      return null;
    }

    if (validIcdSnomedConceptMaps.size() == 2) {
      var primarySecondaryIcdCodesPair =
          setIcdPairs(validIcdSnomedConceptMaps, conditionLogicId, conditionSourceIdentifier);
      wrapper.setPostProcessMap(primarySecondaryIcdCodesPair);
    }

    var icdCodingList =
        getCodingBySystemUrl(uncheckedDiagnoseCodingList, fhirSystems.getDiagnoseCode());
    Coding icdCoding = null;
    if (!icdCodingList.isEmpty()) {
      icdCoding = icdCodingList.get(0);
    }

    Pair<String, Integer> icdBodyLocalizationConcepts =
        getBodySiteLocalizationConcepts(
            icdCoding, srcCondition, diagnoseOnset.getStartDateTime().toLocalDate());
    Coding diagnosticConfidence = getDiagnosticConfidence(icdCoding, conditionLogicId);

    var severityCoding = getSeverity(srcCondition);
    var stageCoding = getStage(srcCondition);
    var diagnosticConfidenceConcept = getDiagnosticConfidenceConcept(diagnosticConfidence);

    for (var validDiagnoseConcept : validIcdSnomedConceptMaps) {
      icdProcessor(
          validDiagnoseConcept,
          wrapper,
          diagnoseOnset,
          diagnosticConfidenceConcept,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId);
      setBodySiteLocalization(
          wrapper,
          validDiagnoseConcept,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          icdBodyLocalizationConcepts);
      setDiagnoseMetaInfo(
          wrapper,
          validDiagnoseConcept,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          severityCoding,
          "severity");

      setDiagnoseMetaInfo(
          wrapper,
          validDiagnoseConcept,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          stageCoding,
          "stage");
    }
    return wrapper;
  }

  /**
   * Create new entry in Observation for severity of a diagnosis, and create new entry in
   * POST_PROCESS_MAP for referencing later.
   *
   * @param wrapper the OMOP model wrapper
   * @param icdSnomedMapList a List of pairs of ICD-Snomed mapping
   * @param omopConcept extracted Concept from OMOP
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId the visit_occurrence_id of the referenced FHIR Encounter resource from
   *     VISIT_OCCURRENCE table in OMOP CDM
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @param severityCoding coding of severity information from FHIR Condition resource
   */
  private void setDiagnoseMetaInfo(
      OmopModelWrapper wrapper,
      Pair<String, List<IcdSnomedDomainLookup>> diagnoseCodePair,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset,
      Coding diagnoseMetaInfoCoding,
      String metaInfoType) {
    if (diagnoseMetaInfoCoding == null) {
      return;
    }
    var diagnoseMetaInfoConcept =
        findOmopConcepts.getConcepts(
            diagnoseMetaInfoCoding,
            diagnoseOnset.getStartDateTime().toLocalDate(),
            bulkload,
            dbMappings);
    var diagnoseMetaInfo =
        createDiagnoseMetaInfo(
            diagnoseMetaInfoConcept,
            conditionLogicId,
            conditionSourceIdentifier,
            personId,
            visitOccId,
            diagnoseOnset,
            metaInfoType);
    if (diagnoseMetaInfo == null) {
      return;
    }
    //    wrapper.getObservation().add(diagnoseMetaInfo);
    addToObservationList(wrapper.getObservation(), diagnoseMetaInfo);
    ArrayList<PostProcessMap> diagnoseMetaInfoReference = new ArrayList<>();
    var icdCode = diagnoseCodePair.getLeft();
    var icdSnomedMap = diagnoseCodePair.getRight();

    for (var icdSnomed : icdSnomedMap) {

      diagnoseMetaInfoReference.add(
          PostProcessMap.builder()
              .type(ResourceType.CONDITION.name())
              .dataOne(diagnoseMetaInfo.getObservationSourceValue() + ":Observation")
              .dataTwo(icdCode + ":" + icdSnomed.getSnomedDomainId())
              .omopId(personId)
              .omopTable(metaInfoType)
              .fhirLogicalId(conditionLogicId)
              .fhirIdentifier(conditionSourceIdentifier)
              .build());
    }

    if (diagnoseMetaInfoReference.isEmpty()) {
      return;
    }
    wrapper.getPostProcessMap().addAll(diagnoseMetaInfoReference);
  }

  /**
   * Create new entry in Observation for bodySite or site localization of a diagnosis, and create
   * new entry in POST_PROCESS_MAP for referencing later.
   *
   * @param wrapper the OMOP model wrapper
   * @param icdSnomedMapList a List of pairs of ICD-Snomed mapping
   * @param omopConcept extracted Concept from OMOP
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId the visit_occurrence_id of the referenced FHIR Encounter resource from
   *     VISIT_OCCURRENCE table in OMOP CDM
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @param icdBodyLocalizationConcept a pair of the body site or site localization name and OMOP
   *     concept_id
   */
  private void setBodySiteLocalization(
      OmopModelWrapper wrapper,
      Pair<String, List<IcdSnomedDomainLookup>> diagnoseCodePair,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset,
      Pair<String, Integer> icdBodyLocalizationConcept) {
    if (diagnoseCodePair == null) {
      return;
    }

    var siteLocalization =
        createSiteLocalization(
            icdBodyLocalizationConcept,
            conditionLogicId,
            conditionSourceIdentifier,
            personId,
            visitOccId,
            diagnoseOnset);
    if (siteLocalization == null) {
      return;
    }
    addToObservationList(wrapper.getObservation(), siteLocalization);
    var icdCode = diagnoseCodePair.getLeft();
    var icdSnomedMap = diagnoseCodePair.getRight();

    for (var icdSnomed : icdSnomedMap) {

      var icdSiteLocalization =
          PostProcessMap.builder()
              .type(ResourceType.CONDITION.name())
              .dataOne(siteLocalization.getObservationSourceValue() + ":Observation")
              .dataTwo(icdCode + ":" + icdSnomed.getSnomedDomainId())
              .omopId(personId)
              .omopTable("site_localization")
              .fhirLogicalId(conditionLogicId)
              .fhirIdentifier(conditionSourceIdentifier)
              .build();
      addToPostProcessMap(wrapper.getPostProcessMap(), icdSiteLocalization);
    }
  }
  /**
   * Processes information from FHIR Condition resource and transforms them into records OMOP CDM
   * tables.
   *
   * @param singlePair one pair of ICD code and its OMOP concept_id and domain information
   * @param omopConcept extracted Concept from OMOP
   * @param wrapper the OMOP model wrapper
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @param diagnosticConfidenceConcept
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visiOccId visit_occurrence_id of the referenced FHIR Encounter resource
   */
  private void icdProcessor(
      Pair<String, List<IcdSnomedDomainLookup>> diagnoseCodePair,
      OmopModelWrapper wrapper,
      ResourceOnset diagnoseOnset,
      SourceToConceptMap diagnosticConfidenceConcept,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visiOccId) {
    if (diagnoseCodePair == null) {
      return;
    }

    var rawIcdCode = diagnoseCodePair.getLeft();
    var icdSnomedMaps = diagnoseCodePair.getRight();

    for (var icdCodeCheck : icdSnomedMaps) {
      var domain = icdCodeCheck.getSnomedDomainId();
      setDiagnoses(
          diagnosticConfidenceConcept,
          wrapper,
          diagnoseOnset,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visiOccId,
          rawIcdCode,
          icdCodeCheck.getSnomedConceptId(),
          icdCodeCheck.getIcdGmConceptId(),
          domain);
    }
  }

  /**
   * @param codingList
   * @param codingSystemUrl
   * @return
   */
  private List<Coding> getCodingBySystemUrl(List<Coding> codingList, List<String> codingSystemUrl) {
    List<Coding> diagnoseCodingList = new ArrayList<>();
    for (var coding : codingList) {
      var systemUrl = coding.getSystem();
      if (codingSystemUrl.contains(systemUrl)) {
        diagnoseCodingList.add(coding);
      }
    }
    return diagnoseCodingList;
  }

  /**
   * Retrieve diagnose status from Condition FHIR resource based on its clinical and verification
   * status.
   *
   * @param clinicalStatusValue
   * @param verificationStatusValue
   */
  private boolean diagnoseStatusProvement(
      Coding clinicalStatusValue, Coding verificationStatusValue) {
    if (clinicalStatusValue == null && verificationStatusValue == null) {
      return false;
    }
    if (clinicalStatusValue != null
        && clinicalStatusValue.getCode().equals("active")
        && verificationStatusValue == null) {
      return true;
    }

    if (clinicalStatusValue != null
        && clinicalStatusValue.getCode().equals("active")
        && verificationStatusValue != null
        && verificationStatusValue.getCode().equals("confirmed")) {
      return true;
    }
    return false;
  }

  /**
   * Retrieve verification status from Condition FHIR resource.
   *
   * @param srcCondition
   * @return
   */
  private Coding getVerificationStatusValue(Condition srcCondition) {
    var verificationStatusElement =
        checkDataAbsentReason.getValue(srcCondition.getVerificationStatus());
    if (verificationStatusElement == null) {
      return null;
    }
    var verificationStatusValue =
        verificationStatusElement.getCoding().stream()
            .filter(status -> fhirSystems.getVerificationStatus().contains(status.getSystem()))
            .findFirst();
    if (verificationStatusValue.isPresent()) {
      return verificationStatusValue.get();
    }
    return null;
  }

  /**
   * Retrive clinical status from Condition FHIR resource.
   *
   * @param srcCondition
   * @return
   */
  private Coding getClinicalStatus(Condition srcCondition) {
    var statusElement = checkDataAbsentReason.getValue(srcCondition.getClinicalStatus());
    if (statusElement == null) {
      return null;
    }
    var statusValue =
        statusElement.getCoding().stream()
            .filter(status -> status.getSystem().equals(fhirSystems.getClinicalStatus()))
            .findFirst();
    if (statusValue.isPresent()) {
      return statusValue.get();
    }
    return null;
  }

  /**
   * Extract valid pairs of ICD code and its OMOP concept_id and domain information as a list
   *
   * @param uncheckedIcds unchecked ICD codes
   * @param diagnoseDate the start date of diagnose
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @return a list of valid pairs of ICD code and its OMOP concept_id and domain information
   */
  private List<Pair<String, List<IcdSnomedDomainLookup>>> getValidIcdSnomedCodes(
      List<Coding> uncheckedCodings, LocalDate diagnoseDate, String conditionLogicId) {
    if (uncheckedCodings.isEmpty()) {
      return Collections.emptyList();
    }

    var uncheckedSnomedCodings =
        getCodingBySystemUrl(uncheckedCodings, List.of(fhirSystems.getSnomed()));

    var uncheckedIcdCodings = getCodingBySystemUrl(uncheckedCodings, fhirSystems.getDiagnoseCode());
    if (uncheckedIcdCodings.isEmpty() && uncheckedSnomedCodings.isEmpty()) {
      return Collections.emptyList();
    }

    var uncheckedSnomedCoding =
        uncheckedSnomedCodings.isEmpty() ? null : uncheckedSnomedCodings.get(0);
    var snomedConcept =
        findOmopConcepts.getConcepts(uncheckedSnomedCoding, diagnoseDate, bulkload, dbMappings);

    List<Pair<String, List<IcdSnomedDomainLookup>>> validIcdSnomedConceptMaps = new ArrayList<>();

    for (var uncheckedIcdCoding : uncheckedIcdCodings) {
      var icdSnomedConcept =
          findOmopConcepts.getIcdSnomedConcepts(
              uncheckedIcdCoding, diagnoseDate, bulkload, dbMappings);
      if (icdSnomedConcept.isEmpty() && snomedConcept == null) {
        return Collections.emptyList();
      }

      if (uncheckedIcdCoding != null
          && uncheckedSnomedCoding != null
          && !icdSnomedConcept.isEmpty()
          && snomedConcept != null) {
        // ICD code and SnomedCode exist at the same time
        for (var icdSnomedMap : icdSnomedConcept) {
          icdSnomedMap.setSnomedConceptId(snomedConcept.getConceptId());
          icdSnomedMap.setSnomedDomainId(snomedConcept.getDomainId());
          validIcdSnomedConceptMaps.add(
              Pair.of(uncheckedIcdCoding.getCode(), List.of(icdSnomedMap)));
        }
      } else if (uncheckedIcdCoding != null && !icdSnomedConcept.isEmpty()) {

        validIcdSnomedConceptMaps.add(Pair.of(uncheckedIcdCoding.getCode(), icdSnomedConcept));

      } else if (uncheckedSnomedCoding != null && snomedConcept != null) {
        var snomedFormatedConcept =
            IcdSnomedDomainLookup.builder()
                .icdGmCode(snomedConcept.getConceptCode())
                .icdGmConceptId(snomedConcept.getConceptId())
                .snomedConceptId(snomedConcept.getConceptId())
                .snomedDomainId(snomedConcept.getDomainId())
                .build();
        validIcdSnomedConceptMaps.add(
            Pair.of(uncheckedSnomedCoding.getCode(), List.of(snomedFormatedConcept)));
      } else {
        return Collections.emptyList();
      }
    }

    return validIcdSnomedConceptMaps;
  }

  /**
   * Extracts information of diagnostic codes from the FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @return Coding part of the FHIR Condition resource, which contains diagnostic codes
   */
  private List<Coding> getDiagnoseCoding(Condition srcCondition) {
    var diagnoseCodeableConcept = checkDataAbsentReason.getValue(srcCondition.getCode());
    if (diagnoseCodeableConcept == null) {
      return Collections.emptyList();
    }
    var diagnoseCodings = diagnoseCodeableConcept.getCoding();
    if (diagnoseCodings.isEmpty()) {
      return Collections.emptyList();
    }

    List<Coding> diagnoseCodingList = new ArrayList<>();

    for (var diagnoseCoding : diagnoseCodings) {
      if (diagnoseCodeExists(diagnoseCoding)) {
        if (!fhirSystems.getIcd10gm().contains(diagnoseCoding.getSystem())) {
          diagnoseCodingList.add(diagnoseCoding);
        } else {
          var sourceCodes = diagnoseCoding.getCode().strip();
          if (!sourceCodes.contains(" ")) {
            diagnoseCodingList.add(diagnoseCoding);
          } else {
            var codeArr = Arrays.asList(sourceCodes.split(" ", 2));
            for (var code : codeArr) {
              var element =
                  new Coding()
                      .setCode(code)
                      .setSystem(diagnoseCoding.getSystem())
                      .setVersionElement(diagnoseCoding.getVersionElement())
                      .setExtension(diagnoseCoding.getExtension());
              var singleCoding = element.castToCoding(element);
              diagnoseCodingList.add(singleCoding);
            }
          }
        }
      }
    }

    return diagnoseCodingList;
  }

  /**
   * Check whether the code of diagnose exists
   *
   * @param diagnoseCoding Coding part of the FHIR Condition resource, which contains diagnostic
   *     codes
   * @return a boolean value
   */
  private boolean diagnoseCodeExists(Coding diagnoseCoding) {
    var diagnoseCode = checkDataAbsentReason.getValue(diagnoseCoding.getCodeElement());

    if (Strings.isNullOrEmpty(diagnoseCode)) {
      return false;
    }
    return true;
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR Condition
   * resource.
   *
   * @param srcCondition FHIR Condition resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Condition srcCondition, String conditionLogicId, String conditionId) {
    var patientReferenceIdentifier = fhirReferenceUtils.getSubjectReferenceIdentifier(srcCondition);
    var patientReferenceLogicalId = fhirReferenceUtils.getSubjectReferenceLogicalId(srcCondition);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, conditionLogicId, conditionId);
  }

  /**
   * Returns the visit_occurrence_id of the referenced FHIR Encounter resource for the processed
   * FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @param conditionId logical id of the FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(Condition srcCondition, String conditionId, Long personId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcCondition);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcCondition);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier, encounterReferenceLogicalId, personId, conditionId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for [Condition]: {}.", conditionId);
    }

    return visitOccId;
  }

  /**
   * Extracts date time information from the FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @return start date time and end date time of the FHIR Condition resource
   */
  private ResourceOnset getConditionOnset(Condition srcCondition) {
    var resourceOnset = new ResourceOnset();

    if (srcCondition.hasOnsetDateTimeType()) {
      var onsetDateTimeType = srcCondition.getOnsetDateTimeType();
      if (!onsetDateTimeType.isEmpty()) {
        resourceOnset.setStartDateTime(
            new Timestamp(onsetDateTimeType.getValue().getTime()).toLocalDateTime());
        return resourceOnset;
      }
    }

    if (srcCondition.hasOnsetPeriod()) {
      var onsetPeriod = srcCondition.getOnsetPeriod();
      if (!onsetPeriod.isEmpty()) {
        if (onsetPeriod.getStart() != null) {
          resourceOnset.setStartDateTime(
              new Timestamp(onsetPeriod.getStart().getTime()).toLocalDateTime());
        }
        if (onsetPeriod.getEnd() != null) {
          resourceOnset.setEndDateTime(
              new Timestamp(onsetPeriod.getEnd().getTime()).toLocalDateTime());
        }
        return resourceOnset;
      }
    }

    var recordedDateElement = srcCondition.getRecordedDateElement();
    if (!recordedDateElement.isEmpty()) {
      var recordedDate = checkDataAbsentReason.getValue(recordedDateElement);
      if (recordedDate != null) {
        resourceOnset.setStartDateTime(recordedDate);
        return resourceOnset;
      }
    }
    return resourceOnset;
  }

  /**
   * Write diagnose information into corrected OMOP tables based on their domains.
   *
   * @param diagnosticConfidenceConcept OMOP concept for diagnostic confidence
   * @param wrapper the OMOP model wrapper
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visiOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param rawIcdCode ICD code with special characters
   * @param diagnoseConceptId the OMOP concept_id of diagnose Code as concept_id
   * @param diagnoseSourceConceptId the OMOP concept_id of diagnose Code as source_concept_id
   * @param domain the OMOP domain of the diagnose code
   */
  private void setDiagnoses(
      SourceToConceptMap diagnosticConfidenceConcept,
      OmopModelWrapper wrapper,
      ResourceOnset diagnoseOnset,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visiOccId,
      String rawIcdCode,
      Integer diagnoseConceptId,
      Integer diagnoseSourceConceptId,
      String domain) {
    switch (domain) {
      case OMOP_DOMAIN_CONDITION:
        var condition =
            setUpCondition(
                diagnoseOnset,
                diagnoseConceptId,
                diagnoseSourceConceptId,
                rawIcdCode,
                diagnosticConfidenceConcept,
                personId,
                visiOccId,
                conditionLogicId,
                conditionSourceIdentifier);

        wrapper.getConditionOccurrence().add(condition);

        break;
      case OMOP_DOMAIN_OBSERVATION:
        var observation =
            setUpObservation(
                diagnoseOnset.getStartDateTime(),
                diagnoseConceptId,
                diagnoseSourceConceptId,
                rawIcdCode,
                diagnosticConfidenceConcept,
                personId,
                visiOccId,
                conditionLogicId,
                conditionSourceIdentifier);

        wrapper.getObservation().add(observation);

        break;
      case OMOP_DOMAIN_PROCEDURE:
        var procedure =
            setUpProcedure(
                diagnoseOnset.getStartDateTime(),
                diagnoseConceptId,
                diagnoseSourceConceptId,
                rawIcdCode,
                diagnosticConfidenceConcept,
                personId,
                visiOccId,
                conditionLogicId,
                conditionSourceIdentifier);

        wrapper.getProcedureOccurrence().add(procedure);

        break;
      case OMOP_DOMAIN_MEASUREMENT:
        var measurement =
            setUpMeasurement(
                diagnoseOnset.getStartDateTime(),
                diagnoseConceptId,
                diagnoseSourceConceptId,
                rawIcdCode,
                diagnosticConfidenceConcept,
                personId,
                visiOccId,
                conditionLogicId,
                conditionSourceIdentifier);

        wrapper.getMeasurement().add(measurement);

        break;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported domain %s", domain));
    }
  }

  /**
   * Extracts diagnostic confidence information from FHIR Condition resource.
   *
   * @param icdCoding part of the FHIR Condition resource, which contains ICD codes
   * @param conditionId logical id of the FHIR Condition resource
   * @return diagnostic confidence from FHIR Condition resource
   */
  private Coding getDiagnosticConfidence(Coding icdCoding, String conditionId) {

    if (!icdCoding.hasExtension(fhirSystems.getDiagnosticConfidence())) {
      //      log.debug("No [Diagnostic confidence] found for Condition [{}].", conditionId);
      return null;
    }
    var diagnosticConfidenceType =
        icdCoding.getExtensionByUrl(fhirSystems.getDiagnosticConfidence()).getValue();

    if (diagnosticConfidenceType == null) {
      return null;
    }

    //        var diagnosticConfidenceType =
    //            icdCoding.getExtensionByUrl(fhirSystems.getDiagnosticConfidence()).getValue();
    var diagnosticConfidence = diagnosticConfidenceType.castToCoding(diagnosticConfidenceType);
    if (!diagnosticConfidence.hasCode() || Strings.isNullOrEmpty(diagnosticConfidence.getCode())) {
      log.debug("No [Diagnostic confidence] found for Condition [{}].", conditionId);
      return null;
    }
    return diagnosticConfidence;
  }

  /**
   * Deletes FHIR Condition resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   */
  private void deleteExistingConditionEntry(
      String conditionLogicId, String conditionSourceIdentifier) {
    if (!Strings.isNullOrEmpty(conditionLogicId)) {
      conditionService.deleteExistingConditionsByFhirLogicalId(conditionLogicId);
    } else {
      conditionService.deleteExistingConditionsByFhirIdentifier(conditionSourceIdentifier);
    }
  }

  /**
   * Deletes rank and use information from post_process_map table using fhir_logical_id and
   * fhir_identifier
   *
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   */
  private void deleteExistingPostProcessMapEntry(
      String conditionLogicId, String conditionSourceIdentifier) {
    if (!Strings.isNullOrEmpty(conditionLogicId)) {
      conditionService.deleteExistingPpmEntriesByFhirLogicalId(conditionLogicId);
    } else {
      conditionService.deleteExistingPpmEntriesByFhirIdentifier(conditionSourceIdentifier);
    }
  }

  /**
   * Updates flag for rank and use information in post_process_map table using fhir_logical_id and
   * fhir_identifier
   *
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   */
  private void updateExistingPostProcessMapEntry(
      String conditionLogicId, String conditionSourceIdentifier) {
    if (!Strings.isNullOrEmpty(conditionLogicId)) {
      conditionService.updateExistingPpmEntriesByFhirLogicalId(conditionLogicId);
    } else {
      conditionService.updateExistingPpmEntriesByFhirIdentifier(conditionSourceIdentifier);
    }
  }

  /**
   * Creates a new record of the condition_occurrence table in OMOP CDM for the processed FHIR
   * Condition resource. The extracted ICD code of the FHIR Condition resource belongs to the
   * Condition domain in OMOP CDM.
   *
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @param diagnoseConceptId the OMOP concept_id of diagnose Code as concept_id
   * @param diagnoseSourceConceptId the OMOP concept_id of diagnose Code as source_concept_id
   * @param rawIcdCode ICD code with special characters
   * @param diagnosticConfidenceConcept the OMOP concept of diagnostic confidence
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return new record of the condition_occurrence table in OMOP CDM for the processed FHIR
   *     Condition resource
   */
  private ConditionOccurrence setUpCondition(
      ResourceOnset diagnoseOnset,
      Integer diagnoseConceptId,
      Integer diagnoseSourceConceptId,
      String rawIcdCode,
      SourceToConceptMap diagnosticConfidenceConcept,
      Long personId,
      Long visitOccId,
      String conditionLogicId,
      String conditionSourceIdentifier) {
    var startDatetime = diagnoseOnset.getStartDateTime();
    var endDatetime = diagnoseOnset.getEndDateTime();

    var newConditionOcc =
        ConditionOccurrence.builder()
            .personId(personId)
            .conditionStartDate(startDatetime.toLocalDate())
            .conditionStartDatetime(startDatetime)
            .conditionEndDatetime(endDatetime)
            .conditionEndDate(endDatetime != null ? endDatetime.toLocalDate() : null)
            .visitOccurrenceId(visitOccId)
            .conditionSourceConceptId(diagnoseSourceConceptId)
            .conditionConceptId(diagnoseConceptId)
            .conditionTypeConceptId(CONCEPT_EHR)
            .conditionSourceValue(rawIcdCode)
            .fhirLogicalId(conditionLogicId)
            .fhirIdentifier(conditionSourceIdentifier)
            .build();

    if (diagnosticConfidenceConcept != null
        && diagnosticConfidenceConcept.getTargetConceptId() != null) {

      newConditionOcc.setConditionStatusSourceValue(diagnosticConfidenceConcept.getSourceCode());
      newConditionOcc.setConditionStatusConceptId(diagnosticConfidenceConcept.getTargetConceptId());
    }

    return newConditionOcc;
  }

  /**
   * Find the concept_id of diagnostic confidence.
   *
   * @param diagnosticConfidenceCoding Diagnostic confidence of Condition FHIR Resource
   * @return a entry of SOURCE_TO_CONCEPT_MAP for diagnostic confidence
   */
  private SourceToConceptMap getDiagnosticConfidenceConcept(Coding diagnosticConfidenceCoding) {
    if (diagnosticConfidenceCoding == null) {
      return null;
    }

    return findOmopConcepts.getCustomConcepts(
        diagnosticConfidenceCoding, SOURCE_VOCABULARY_ID_DIAGNOSTIC_CONFIDENCE, dbMappings);
  }

  /**
   * Creates a new record of the observation table in OMOP CDM for the processed FHIR Condition
   * resource. The extracted ICD code of the FHIR Condition resource belongs to the Observation
   * domain in OMOP CDM.
   *
   * @param startDatetime start date time of the FHIR Condition resource
   * @param diagnoseConceptId the OMOP concept_id of diagnose Code as concept_id
   * @param diagnoseSourceConceptId the OMOP concept_id of diagnose Code as source_concept_id
   * @param rawIcdCode ICD code with special characters
   * @param diagnosticConfidenceConcept the OMOP concept of diagnostic confidence
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return new record of the observation table in OMOP CDM for the processed FHIR Condition
   *     resource
   */
  private OmopObservation setUpObservation(
      LocalDateTime startDatetime,
      Integer diagnoseConceptId,
      Integer diagnoseSourceConceptId,
      String rawIcdCode,
      SourceToConceptMap diagnosticConfidenceConcept,
      Long personId,
      Long visitOccId,
      String conditionLogicId,
      String conditionSourceIdentifier) {

    var newObservation =
        OmopObservation.builder()
            .personId(personId)
            .observationDate(startDatetime.toLocalDate())
            .observationDatetime(startDatetime)
            .visitOccurrenceId(visitOccId)
            .observationSourceConceptId(diagnoseSourceConceptId)
            .observationConceptId(diagnoseConceptId)
            .observationTypeConceptId(CONCEPT_EHR)
            .observationSourceValue(rawIcdCode)
            .fhirLogicalId(conditionLogicId)
            .fhirIdentifier(conditionSourceIdentifier)
            .build();

    if (diagnosticConfidenceConcept != null) {

      newObservation.setQualifierSourceValue(diagnosticConfidenceConcept.getSourceCode());

      newObservation.setQualifierConceptId(diagnosticConfidenceConcept.getTargetConceptId());
    }

    return newObservation;
  }

  /**
   * Creates a new record of the procedure_occurrence table in OMOP CDM for the processed FHIR
   * Condition resource. The extracted ICD code of the FHIR Condition resource belongs to the
   * Procedure domain in OMOP CDM.
   *
   * @param startDatetime start date time of the FHIR Condition resource
   * @param diagnoseConceptId the OMOP concept_id of diagnose Code as concept_id
   * @param diagnoseSourceConceptId the OMOP concept_id of diagnose Code as source_concept_id
   * @param rawIcdCode ICD code with special characters
   * @param diagnosticConfidenceConcept the OMOP concept of diagnostic confidence
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return new record of the procedure_occurrence table in OMOP CDM for the processed FHIR
   *     Condition resource
   */
  private ProcedureOccurrence setUpProcedure(
      LocalDateTime startDatetime,
      Integer diagnoseConceptId,
      Integer diagnoseSourceConceptId,
      String rawIcdCode,
      SourceToConceptMap diagnosticConfidenceConcept,
      Long personId,
      Long visitOccId,
      String conditionLogicId,
      String conditionSourceIdentifier) {

    var newProcedureOcc =
        ProcedureOccurrence.builder()
            .personId(personId)
            .procedureDate(startDatetime.toLocalDate())
            .procedureDatetime(startDatetime)
            .visitOccurrenceId(visitOccId)
            .procedureSourceConceptId(diagnoseSourceConceptId)
            .procedureConceptId(diagnoseConceptId)
            .procedureTypeConceptId(CONCEPT_EHR)
            .procedureSourceValue(rawIcdCode)
            .fhirLogicalId(conditionLogicId)
            .fhirIdentifier(conditionSourceIdentifier)
            .build();

    if (diagnosticConfidenceConcept != null) {
      newProcedureOcc.setModifierSourceValue(diagnosticConfidenceConcept.getSourceCode());

      newProcedureOcc.setModifierConceptId(diagnosticConfidenceConcept.getTargetConceptId());
    }

    return newProcedureOcc;
  }

  /**
   * Creates a new record of the measurement table in OMOP CDM for the processed FHIR Condition
   * resource. The extracted ICD code of the FHIR Condition resource belongs to the Measurement
   * domain in OMOP CDM.
   *
   * @param startDatetime start date time of the FHIR Condition resource
   * @param diagnoseConceptId the OMOP concept_id of diagnose Code as concept_id
   * @param diagnoseSourceConceptId the OMOP concept_id of diagnose Code as source_concept_id
   * @param rawIcdCode ICD code with special characters
   * @param diagnosticConfidenceConcept the OMOP concept of diagnostic confidence
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return new record of the measurement table in OMOP CDM for the processed FHIR Condition
   *     resource
   */
  private Measurement setUpMeasurement(
      LocalDateTime startDatetime,
      Integer diagnoseConceptId,
      Integer diagnoseSourceConceptId,
      String rawIcdCode,
      SourceToConceptMap diagnosticConfidenceConcept,
      Long personId,
      Long visitOccId,
      String conditionLogicId,
      String conditionSourceIdentifier) {

    var newMeasurement =
        Measurement.builder()
            .personId(personId)
            .measurementDate(startDatetime.toLocalDate())
            .measurementDatetime(startDatetime)
            .visitOccurrenceId(visitOccId)
            .measurementSourceConceptId(diagnoseSourceConceptId)
            .measurementConceptId(diagnoseConceptId)
            .measurementTypeConceptId(CONCEPT_EHR)
            .measurementSourceValue(rawIcdCode)
            .fhirLogicalId(conditionLogicId)
            .fhirIdentifier(conditionSourceIdentifier)
            .build();

    if (diagnosticConfidenceConcept != null) {
      newMeasurement.setValueSourceValue(diagnosticConfidenceConcept.getSourceCode());

      newMeasurement.setValueAsConceptId(diagnosticConfidenceConcept.getTargetConceptId());
    }

    return newMeasurement;
  }

  /**
   * Creates new records of the post_process_map table in OMOP CDM for ICD pairs.
   *
   * @param icdSnomedPairMaps a list of valid pairs of ICD code and its OMOP concept_id and domain
   *     information
   * @param personIdperson_id of the referenced FHIR Patient resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return list of new records of the post_process_map table in OMOP CDM for the processed FHIR
   *     Condition resource
   */
  private List<PostProcessMap> setIcdPairs(
      List<Pair<String, List<IcdSnomedDomainLookup>>> icdSnomedPairMaps,
      String conditionLogicId,
      String conditionSourceIdentifier) {

    var primary = icdSnomedPairMaps.get(0);
    var secondary = icdSnomedPairMaps.get(1);

    var primaryIcdCode = primary.getLeft();
    var secondaryIcdCode = secondary.getLeft();

    var primaryIcdSnomedMap = primary.getRight();
    var secondaryIcdSnomedMap = secondary.getRight();

    ArrayList<PostProcessMap> icdPairs = new ArrayList<>();

    for (var pri : primaryIcdSnomedMap) {

      for (var sec : secondaryIcdSnomedMap) {
        var ppmIcdPairs =
            PostProcessMap.builder()
                .type(ResourceType.CONDITION.name())
                .dataOne(primaryIcdCode + ":" + pri.getSnomedDomainId())
                .dataTwo(secondaryIcdCode + ":" + sec.getSnomedDomainId())
                .omopId(0L)
                .omopTable("primary_secondary_icd")
                .fhirLogicalId(conditionLogicId)
                .fhirIdentifier(conditionSourceIdentifier)
                .build();
        if (!icdPairs.contains(ppmIcdPairs)) {
          icdPairs.add(ppmIcdPairs);
        }
      }
    }
    return icdPairs;
  }

  /**
   * Creates new records of the post_process_map table in OMOP CDM for extracted severity
   * information from the processed FHIR Condition resource.
   *
   * @param icdSnomedMapList a list of valid pairs of ICD code and its OMOP concept_id and domain
   *     information
   * @param omopConcept extracted Concept from OMOP
   * @param personId person_id of the referenced FHIR Patient resource
   * @param severity severity information from the processed FHIR Condition resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return list of new records of the post_process_map table in OMOP CDM for the processed FHIR
   *     Condition resource
   */

  /**
   * Creates a new record of the observation table in OMOP CDM for extracted site localization
   * information from the processed FHIR Condition resource.
   *
   * @param icdBodyLocalization a pair of the body site or site localization name and OMOP
   *     concept_id
   * @param conditionLogicId logical id of FHIR Condition resource
   * @param conditionSourceIdentifier identifier of FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @return new record of the observation table in OMOP CDM for extracted site localization
   *     information from the processed FHIR Condition resource
   */
  private OmopObservation createSiteLocalization(
      Pair<String, Integer> icdBodyLocalization,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset) {

    if (icdBodyLocalization != null && !Strings.isNullOrEmpty(icdBodyLocalization.getLeft())) {

      return OmopObservation.builder()
          .personId(personId)
          .observationDate(diagnoseOnset.getStartDateTime().toLocalDate())
          .observationDatetime(diagnoseOnset.getStartDateTime())
          .observationTypeConceptId(CONCEPT_EHR)
          .observationConceptId(icdBodyLocalization.getRight())
          .valueAsString(icdBodyLocalization.getLeft())
          .observationSourceValue(icdBodyLocalization.getLeft())
          .observationSourceConceptId(icdBodyLocalization.getRight())
          .visitOccurrenceId(visitOccId)
          .fhirLogicalId(conditionLogicId)
          .fhirIdentifier(conditionSourceIdentifier)
          .build();
    }
    return null;
  }

  /**
   * Creates a new record of the observation table in OMOP CDM for extracted severity information
   * from the processed FHIR Condition resource.
   *
   * @param diagnoseMetaInfoConcept concept of diagnose meta information
   * @param conditionLogicId logical id of FHIR Condition resource
   * @param conditionSourceIdentifier identifier of FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param diagnoseOnset start date time and end date time of the FHIR Condition resource
   * @return new record of the observation table in OMOP CDM for extracted site localization
   *     information from the processed FHIR Condition resource
   */
  private OmopObservation createDiagnoseMetaInfo(
      Concept diagnoseMetaInfoConcept,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset,
      String metaInfoType) {

    if (diagnoseMetaInfoConcept == null) {

      return null;
    }
    return OmopObservation.builder()
        .personId(personId)
        .observationDate(diagnoseOnset.getStartDateTime().toLocalDate())
        .observationDatetime(diagnoseOnset.getStartDateTime())
        .observationTypeConceptId(CONCEPT_EHR)
        .observationConceptId(diagnoseMetaInfoConcept.getConceptId())
        .valueAsString(diagnoseMetaInfoConcept.getConceptCode())
        .observationSourceValue(diagnoseMetaInfoConcept.getConceptCode())
        .observationSourceConceptId(diagnoseMetaInfoConcept.getConceptId())
        .qualifierConceptId(metaInfoType.equals("stage") ? CONCEPT_STAGE : CONCEPT_SEVERITY)
        .visitOccurrenceId(visitOccId)
        .fhirLogicalId(conditionLogicId)
        .fhirIdentifier(conditionSourceIdentifier)
        .build();
  }

  /**
   * Extract the site localization name and its OMOP concept_id
   *
   * @param icdCoding part of the FHIR Condition resource, which contains ICD codes
   * @param srcCondition FHIR Condition resource
   * @param diagnoseDate start date time of the FHIR Condition resource
   * @return a pair of the site localization name and its OMOP concept_id
   */
  private Pair<String, Integer> getBodySiteLocalizationConcepts(
      Coding icdCoding, Condition srcCondition, LocalDate diagnoseDate) {
    var icdSiteLocalization = getSiteLocalization(icdCoding);
    var diagnoseBodySite = getBodySite(srcCondition);

    if (icdSiteLocalization == null && diagnoseBodySite == null) {
      return null;
    } else if (icdSiteLocalization != null) {
      var icdSiteLocalizationConcept =
          findOmopConcepts.getCustomConcepts(
              icdSiteLocalization, SOURCE_VOCABULARY_ID_ICD_LOCALIZATION, dbMappings);
      return Pair.of(
          icdSiteLocalization.getCode(), icdSiteLocalizationConcept.getTargetConceptId());
    } else {
      var diagnoseBodySiteConcept =
          findOmopConcepts.getConcepts(diagnoseBodySite, diagnoseDate, bulkload, dbMappings);
      if (diagnoseBodySiteConcept != null) {
        return Pair.of(diagnoseBodySite.getCode(), diagnoseBodySiteConcept.getConceptId());
      }
    }
    return null;
  }

  /**
   * Extracts site localization information from the processed FHIR Condition resource.
   *
   * @param icdCoding part of the FHIR Condition resource, which contains ICD codes
   * @return site localization from FHIR Condition resource
   */
  private Coding getSiteLocalization(Coding icdCoding) {

    Extension siteLocalizationCodingExtension =
        icdCoding.getExtensionByUrl(fhirSystems.getSiteLocalizationExtension());

    if (siteLocalizationCodingExtension == null) {
      return null;
    }
    var siteLocalizationCoding =
        siteLocalizationCodingExtension
            .getValue()
            .castToCoding(siteLocalizationCodingExtension.getValue());
    var siteLocalizationCode = siteLocalizationCoding.getCode();
    if (Strings.isNullOrEmpty(siteLocalizationCode)) {
      return null;
    }

    return siteLocalizationCoding;
  }

  /**
   * Extracts body site information from the processed FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @return body site from FHIR Condition resource
   */
  private Coding getBodySite(Condition srcCondition) {
    if (!srcCondition.hasBodySite() || srcCondition.getBodySite().isEmpty()) {
      return null;
    }
    var bodySiteCodeable = srcCondition.getBodySite();

    var bodySiteCoding =
        bodySiteCodeable.stream().filter(codeable -> !codeable.getCoding().isEmpty()).findFirst();
    if (bodySiteCoding.isEmpty()) {
      return null;
    }

    var bodySite =
        bodySiteCoding.get().getCoding().stream()
            .filter(coding -> coding.getSystem().equals(fhirSystems.getSnomed()))
            .findFirst();
    if (!bodySite.isPresent()) {
      return null;
    }
    var bodySiteCode = bodySite.get().getCode();
    if (Strings.isNullOrEmpty(bodySiteCode)) {
      return null;
    }
    return bodySite.get();
  }

  /**
   * Extracts stage information from the processed FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @return stage from FHIR Condition resource
   */
  private Coding getStage(Condition srcCondition) {

    if (!srcCondition.hasStage() || srcCondition.getStage().isEmpty()) {
      return null;
    }
    var stage = srcCondition.getStage();

    var stageComponent =
        stage.stream().filter(summary -> !summary.getSummary().isEmpty()).findFirst();
    if (stageComponent.isEmpty()
        || !stageComponent.get().hasSummary()
        || stageComponent.get().getSummary() == null) {
      return null;
    }

    var stageSummaryCodeable = stageComponent.get().getSummary();
    if (stageSummaryCodeable.isEmpty() || !stageSummaryCodeable.hasCoding()) {
      return null;
    }

    var stageSummary =
        stageSummaryCodeable.getCoding().stream()
            .filter(coding -> coding.getSystem().equals(fhirSystems.getSnomed()))
            .findFirst();
    if (!stageSummary.isPresent()) {
      return null;
    }
    var stageCode = stageSummary.get().getCode();
    if (Strings.isNullOrEmpty(stageCode)) {
      return null;
    }
    return stageSummary.get();
  }

  /**
   * Extracts severity information from the processed FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @return severity from FHIR Condition resource
   */
  private Coding getSeverity(Condition srcCondition) {
    if (!srcCondition.hasSeverity()) {
      return null;
    }
    var severityCodeable = srcCondition.getSeverity();
    if (severityCodeable.isEmpty() || !severityCodeable.hasCoding()) {
      return null;
    }

    var severity =
        severityCodeable.getCoding().stream()
            .filter(coding -> coding.getSystem().equals(fhirSystems.getSnomed()))
            .findFirst();
    if (!severity.isPresent()) {
      return null;
    }
    var severityCode = severity.get().getCode();
    if (Strings.isNullOrEmpty(severityCode)) {
      return null;
    }
    return severity.get();
  }
}
