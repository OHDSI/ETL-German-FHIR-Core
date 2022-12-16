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
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ICD10GM;

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
import org.hl7.fhir.r4.model.StringType;
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
import org.springframework.lang.Nullable;
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

    var diagnoseCodingList = getDiagnoseCoding(srcCondition);
    if (diagnoseCodingList.isEmpty()) {
      log.warn("No Diagnose Code found for [Condition]: {}. Skip resource.", conditionLogicId);
      noCodeCounter.increment();
      return null;
    }

    var personId = getPersonId(srcCondition, conditionLogicId);
    if (personId == null) {
      log.warn("No matching [Person] found for [Condition]: {}. Skip resource", conditionLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcCondition, conditionLogicId, personId);

    var diagnoseOnset = getConditionOnset(srcCondition);
    if (diagnoseOnset.getStartDateTime() == null) {
      log.warn("No [Date] found for [Condition]: {}. Skip resource", conditionLogicId);
      noStartDateCounter.increment();
      return null;
    }
    var severityCoding = getSeverity(srcCondition);
    var stageCoding = getStage(srcCondition);

    if (diagnoseCodingList.size() == 1) {
      var diagnoseCoding = diagnoseCodingList.get(0);
      var icdBodyLocalizationConcepts =
          getBodySiteLocalizationConcepts(
              diagnoseCoding, srcCondition, diagnoseOnset.getStartDateTime().toLocalDate());
      var diagnosticConfidence = getDiagnosticConfidence(diagnoseCoding, conditionLogicId);
      var diagnosticConfidenceConcept = getDiagnosticConfidenceConcept(diagnosticConfidence);
      setDiagnoseCodesUsingSingleCoding(
          wrapper,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          diagnoseCoding,
          icdBodyLocalizationConcepts,
          severityCoding,
          stageCoding,
          diagnosticConfidenceConcept);
    } else {
      setDiagnoseCodesUsingMultipleCodings(
          wrapper,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          diagnoseCodingList,
          severityCoding,
          stageCoding,
          srcCondition);
    }

    return wrapper;
  }

  /**
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

  private void setDiagnoseCodesUsingMultipleCodings(
      OmopModelWrapper wrapper,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset,
      List<Coding> diagnoseCodings,
      Coding severityCoding,
      Coding stageCoding,
      Condition srcCondition) {

    Coding uncheckedIcdCoding = null;
    Coding uncheckedSnomedCoding = null;

    for (var uncheckedCoding : diagnoseCodings) {
      var system = uncheckedCoding.getSystem();
      if (fhirSystems.getIcd10gm().contains(system)) {
        uncheckedIcdCoding = uncheckedCoding;
      }
      if (fhirSystems.getSnomed().equals(system)) {
        uncheckedSnomedCoding = uncheckedCoding;
      }
    }
    if (uncheckedIcdCoding == null && uncheckedSnomedCoding == null) {
      return;
    }
    var diagnosticConfidence = getDiagnosticConfidence(uncheckedIcdCoding, conditionLogicId);
    var diagnosticConfidenceConcept = getDiagnosticConfidenceConcept(diagnosticConfidence);
    var icdBodyLocalizationConcepts =
        getBodySiteLocalizationConcepts(
            uncheckedIcdCoding, srcCondition, diagnoseOnset.getStartDateTime().toLocalDate());
    var snomedConcept =
        findOmopConcepts.getConcepts(
            uncheckedSnomedCoding,
            diagnoseOnset.getStartDateTime().toLocalDate(),
            bulkload,
            dbMappings);

    List<Coding> uncheckedIcdCodings =
        splitDiagnoseCodes(uncheckedIcdCoding, uncheckedIcdCoding.getVersionElement());
    var icdSnomedMapPairList =
        getValidIcdCodes(
            uncheckedIcdCodings, diagnoseOnset.getStartDateTime().toLocalDate(), conditionLogicId);
    if (icdSnomedMapPairList.isEmpty() && snomedConcept == null) {
      return;
    } else if (snomedConcept == null) {
      // if a Snomed code does not exist in Concept, the ICD code will be written to OMOP CDM
      setDiagnoseCodesUsingSingleCoding(
          wrapper,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          uncheckedIcdCoding,
          icdBodyLocalizationConcepts,
          severityCoding,
          stageCoding,
          diagnosticConfidenceConcept);
      return;
    } else if (icdSnomedMapPairList.isEmpty()) {
      // if a ICD code does not exist in Concept, the Snomed Code will be written to OMOP CDM
      setDiagnoseCodesUsingSingleCoding(
          wrapper,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId,
          diagnoseOnset,
          uncheckedSnomedCoding,
          icdBodyLocalizationConcepts,
          severityCoding,
          stageCoding,
          diagnosticConfidenceConcept);
      return;
    } else {
      for (var singlePair : icdSnomedMapPairList) {
        // if both ICD and SnomedCode exist, the ICD-SnomedMapping from OMOP CDM will be ignored.
        // The ICD-Snomed Pair from FHIR resource will be used.
        var customIcdSnomedLookUp = singlePair.getRight().get(0);
        customIcdSnomedLookUp.setSnomedConceptId(snomedConcept.getConceptId());
        customIcdSnomedLookUp.setSnomedDomainId(snomedConcept.getDomainId());
        var customSinglePair = Pair.of(singlePair.getLeft(), List.of(customIcdSnomedLookUp));
        icdProcessor(
            customSinglePair,
            null,
            wrapper,
            diagnoseOnset,
            diagnosticConfidenceConcept,
            conditionLogicId,
            conditionSourceIdentifier,
            personId,
            visitOccId);
      }
    }

    //    if (icdSnomedMapPairList.isEmpty()) {
    //      // if a ICD code does not exist in Concept, the SnomedCode will be written to OMOP CDM
    //      setDiagnoseCodesUsingSingleCoding(
    //          wrapper,
    //          conditionLogicId,
    //          conditionSourceIdentifier,
    //          personId,
    //          visitOccId,
    //          diagnoseOnset,
    //          uncheckedSnomedCoding,
    //          icdBodyLocalizationConcepts,
    //          severityCoding,
    //          stageCoding,
    //          diagnosticConfidenceConcept);
    //      return;
    //    }

    //    for (var singlePair : icdSnomedMapPairList) {
    //    for (var uncheckedCoding : uncheckedIcdCodings) {
    //      var icdOmopConcept =
    //          findOmopConcepts.getConcepts(
    //              uncheckedCoding,
    //              diagnoseOnset.getStartDateTime().toLocalDate(),
    //              bulkload,
    //              dbMappings);

    // if a ICD code does not exist in Concept, the SnomedCode will be written to OMOP CDM
    //      if (icdOmopConcept == null) {
    //        setDiagnoseCodesUsingSingleCoding(
    //            wrapper,
    //            conditionLogicId,
    //            conditionSourceIdentifier,
    //            personId,
    //            visitOccId,
    //            diagnoseOnset,
    //            uncheckedSnomedCoding,
    //            icdBodyLocalizationConcepts,
    //            severityCoding,
    //            stageCoding,
    //            diagnosticConfidenceConcept);
    //        return;
    //      }
    //      var customIcdSnomedLookup =
    //          IcdSnomedDomainLookup.builder()
    //              .icdGmCode(icdOmopConcept.getConceptCode())
    //              .icdGmConceptId(icdOmopConcept.getConceptId())
    //              .icdGmValidEndDate(icdOmopConcept.getValidEndDate())
    //              .icdGmValidStartDate(icdOmopConcept.getValidStartDate())
    //              .snomedConceptId(snomedConcept.getConceptId())
    //              //              .snomedDomainId(OMOP_DOMAIN_CONDITION)
    //              .snomedDomainId(snomedConcept.getDomainId())
    //              .build();
    //      icdProcessor(
    //          //          Pair.of(icdOmopConcept.getConceptCode(),
    // List.of(customIcdSnomedLookup)),
    //          singlePair,
    //          null,
    //          wrapper,
    //          diagnoseOnset,
    //          diagnosticConfidenceConcept,
    //          conditionLogicId,
    //          conditionSourceIdentifier,
    //          personId,
    //          visitOccId);
    //    }
    if (icdSnomedMapPairList.size() == 2) {
      var icdPairs = setIcdPairs(icdSnomedMapPairList, conditionLogicId, conditionSourceIdentifier);
      wrapper.setPostProcessMap(icdPairs);
    }

    setBodySiteLocalization(
        wrapper,
        icdSnomedMapPairList,
        snomedConcept,
        conditionLogicId,
        conditionSourceIdentifier,
        personId,
        visitOccId,
        diagnoseOnset,
        icdBodyLocalizationConcepts);

    setDiagnoseMetaInfo(
        wrapper,
        icdSnomedMapPairList,
        snomedConcept,
        conditionLogicId,
        conditionSourceIdentifier,
        personId,
        visitOccId,
        diagnoseOnset,
        severityCoding,
        "severity");

    setDiagnoseMetaInfo(
        wrapper,
        icdSnomedMapPairList,
        snomedConcept,
        conditionLogicId,
        conditionSourceIdentifier,
        personId,
        visitOccId,
        diagnoseOnset,
        stageCoding,
        "stage");
  }

  private void setDiagnoseCodesUsingSingleCoding(
      OmopModelWrapper wrapper,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset,
      Coding diagnoseCoding,
      Pair<String, Integer> icdBodyLocalizationConcepts,
      Coding severityCoding,
      Coding stageCoding,
      SourceToConceptMap diagnosticConfidenceConcept) {

    List<Coding> uncheckedDiagnoseCodes =
        splitDiagnoseCodes(diagnoseCoding, diagnoseCoding.getVersionElement());
    List<Pair<String, List<IcdSnomedDomainLookup>>> icdSnomedMapPairList = null;
    Concept snomedConcept = null;

    if (fhirSystems.getIcd10gm().contains(diagnoseCoding.getSystem())) {
      // for ICD codes

      icdSnomedMapPairList =
          getValidIcdCodes(
              uncheckedDiagnoseCodes,
              diagnoseOnset.getStartDateTime().toLocalDate(),
              conditionLogicId);

      if (icdSnomedMapPairList.isEmpty()) {
        return;
      }
      for (var singlePair : icdSnomedMapPairList) {
        icdProcessor(
            singlePair,
            null,
            wrapper,
            diagnoseOnset,
            diagnosticConfidenceConcept,
            conditionLogicId,
            conditionSourceIdentifier,
            personId,
            visitOccId);
      }

      if (icdSnomedMapPairList.size() == 2) {
        var icdPairs =
            setIcdPairs(icdSnomedMapPairList, conditionLogicId, conditionSourceIdentifier);
        wrapper.setPostProcessMap(icdPairs);
      }
    } else if (fhirSystems.getSnomed().equals(diagnoseCoding.getSystem())) {
      // for Snomed Codes

      snomedConcept =
          findOmopConcepts.getConcepts(
              diagnoseCoding, diagnoseOnset.getStartDateTime().toLocalDate(), bulkload, dbMappings);

      if (snomedConcept == null) {
        return;
      }

      icdProcessor(
          null,
          snomedConcept,
          wrapper,
          diagnoseOnset,
          diagnosticConfidenceConcept,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visitOccId);
    } else {
      return;
    }

    setBodySiteLocalization(
        wrapper,
        icdSnomedMapPairList,
        snomedConcept,
        conditionLogicId,
        conditionSourceIdentifier,
        personId,
        visitOccId,
        diagnoseOnset,
        icdBodyLocalizationConcepts);

    setDiagnoseMetaInfo(
        wrapper,
        icdSnomedMapPairList,
        snomedConcept,
        conditionLogicId,
        conditionSourceIdentifier,
        personId,
        visitOccId,
        diagnoseOnset,
        severityCoding,
        "severity");

    setDiagnoseMetaInfo(
        wrapper,
        icdSnomedMapPairList,
        snomedConcept,
        conditionLogicId,
        conditionSourceIdentifier,
        personId,
        visitOccId,
        diagnoseOnset,
        stageCoding,
        "stage");
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
      @Nullable List<Pair<String, List<IcdSnomedDomainLookup>>> icdSnomedMapList,
      @Nullable Concept omopConcept,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnoseOnset,
      Pair<String, Integer> icdBodyLocalizationConcept) {
    var siteLocalization =
        createSiteLocalization(
            icdBodyLocalizationConcept,
            conditionLogicId,
            conditionSourceIdentifier,
            personId,
            visitOccId,
            diagnoseOnset);
    if (siteLocalization != null) {
      wrapper.getObservation().add(siteLocalization);

      var icdSiteLocalizations =
          setBodySiteLocalizationReference(
              icdSnomedMapList,
              omopConcept,
              personId,
              siteLocalization.getObservationSourceValue(),
              conditionLogicId,
              conditionSourceIdentifier);
      if (!icdSiteLocalizations.isEmpty()) {
        wrapper.getPostProcessMap().addAll(icdSiteLocalizations);
      }
    }
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
      @Nullable List<Pair<String, List<IcdSnomedDomainLookup>>> icdSnomedMapList,
      @Nullable Concept omopConcept,
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
    wrapper.getObservation().add(diagnoseMetaInfo);

    var diagnoseMetaInfoReference =
        setDiagnoseMetaInfoReference(
            icdSnomedMapList,
            omopConcept,
            personId,
            diagnoseMetaInfo.getObservationSourceValue(),
            conditionLogicId,
            conditionSourceIdentifier,
            metaInfoType);
    if (diagnoseMetaInfoReference.isEmpty()) {
      return;
    }
    wrapper.getPostProcessMap().addAll(diagnoseMetaInfoReference);
  }

  /**
   * Extract valid pairs of ICD code and its OMOP concept_id and domain information as a list
   *
   * @param uncheckedIcds unchecked ICD codes
   * @param diagnoseDate the start date of diagnose
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @return a list of valid pairs of ICD code and its OMOP concept_id and domain information
   */
  private List<Pair<String, List<IcdSnomedDomainLookup>>> getValidIcdCodes(
      List<Coding> uncheckedIcds, LocalDate diagnoseDate, String conditionLogicId) {
    if (uncheckedIcds.isEmpty()) {
      return Collections.emptyList();
    }

    List<Pair<String, List<IcdSnomedDomainLookup>>> validIcdSnomedConceptMaps = new ArrayList<>();
    for (var uncheckedCode : uncheckedIcds) {
      List<IcdSnomedDomainLookup> icdSnomedMap =
          findOmopConcepts.getIcdSnomedConcepts(uncheckedCode, diagnoseDate, bulkload, dbMappings);
      if (icdSnomedMap.isEmpty()) {
        log.warn(
            "ICD Code [{}] in [{}] is not valid in OMOP.",
            uncheckedCode.getCode(),
            conditionLogicId);
        return Collections.emptyList();
      }

      validIcdSnomedConceptMaps.add(Pair.of(uncheckedCode.getCode(), icdSnomedMap));
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

      if (fhirSystems.getDiagnoseCode().contains(diagnoseCoding.getSystem())) {
        diagnoseCodingList.add(diagnoseCoding);
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
  private Long getPersonId(Condition srcCondition, String conditionLogicId) {
    var patientReferenceIdentifier = fhirReferenceUtils.getSubjectReferenceIdentifier(srcCondition);
    var patientReferenceLogicalId = fhirReferenceUtils.getSubjectReferenceLogicalId(srcCondition);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, conditionLogicId);
  }

  /**
   * Returns the visit_occurrence_id of the referenced FHIR Encounter resource for the processed
   * FHIR Condition resource.
   *
   * @param srcCondition FHIR Condition resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(Condition srcCondition, String conditionLogicId, Long personId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcCondition);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcCondition);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            srcCondition.getId());
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for [Condition]: {}.", conditionLogicId);
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
   * Separates primary and secondary ICD codes.
   *
   * @param icdSourceCodes string which contains both primary and secondary ICD codes
   * @return list of primary and secondary ICD codes as FHIR Coding-Type
   */
  private List<Coding> splitDiagnoseCodes(Coding diagnoseSourceCoding, StringType version) {
    List<Coding> uncheckedDiagnoseCodes = new ArrayList<>();
    var vocabularyId = findOmopConcepts.getOmopVocabularyId(diagnoseSourceCoding.getSystem());
    if (vocabularyId.equals(VOCABULARY_ICD10GM)) {
      var sourceCodes = diagnoseSourceCoding.getCode().strip();

      if (sourceCodes.contains(" ")) {
        var codeArr = Arrays.asList(sourceCodes.split(" ", 2));
        for (var code : codeArr) {
          var element =
              new Coding()
                  .setCode(code)
                  .setSystem(diagnoseSourceCoding.getSystem())
                  .setVersionElement(version)
                  .setExtension(diagnoseSourceCoding.getExtension());
          var singleCoding = element.castToCoding(element);
          uncheckedDiagnoseCodes.add(singleCoding);
        }

      } else {
        uncheckedDiagnoseCodes.add(diagnoseSourceCoding);
      }
    } else {
      uncheckedDiagnoseCodes.add(diagnoseSourceCoding);
    }
    return uncheckedDiagnoseCodes;
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
      @Nullable Pair<String, List<IcdSnomedDomainLookup>> singlePair,
      @Nullable Concept omopConcept,
      OmopModelWrapper wrapper,
      ResourceOnset diagnoseOnset,
      SourceToConceptMap diagnosticConfidenceConcept,
      String conditionLogicId,
      String conditionSourceIdentifier,
      Long personId,
      Long visiOccId) {

    if (singlePair == null && omopConcept == null) {
      return;
    }

    if (singlePair != null) {
      var rawIcdCode = singlePair.getLeft();
      var icdSnomedMaps = singlePair.getRight();

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
    } else {
      setDiagnoses(
          diagnosticConfidenceConcept,
          wrapper,
          diagnoseOnset,
          conditionLogicId,
          conditionSourceIdentifier,
          personId,
          visiOccId,
          omopConcept.getConceptCode(),
          omopConcept.getConceptId(),
          omopConcept.getConceptId(),
          omopConcept.getDomainId());
    }
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
      log.debug("No [Diagnostic confidence] found for Condition [{}].", conditionId);
      return null;
    }

    var diagnosticConfidenceType =
        icdCoding.getExtensionByUrl(fhirSystems.getDiagnosticConfidence()).getValue();
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
        diagnosticConfidenceCoding.getCode(),
        SOURCE_VOCABULARY_ID_DIAGNOSTIC_CONFIDENCE,
        dbMappings);
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
   * * Creates new records of the post_process_map table in OMOP CDM for extracted site localization
   * information from the processed FHIR Condition resource.
   *
   * @param icdSnomedMapList a list of valid pairs of ICD code and its OMOP concept_id and domain
   *     information
   * @param omopConcept extracted Concept from OMOP
   * @param personId person_id of the referenced FHIR Patient resource
   * @param siteLocalization site localization information from the processed FHIR Condition
   *     resource
   * @param conditionLogicId logical id of the FHIR Condition resource
   * @param conditionSourceIdentifier identifier of the FHIR Condition resource
   * @return list of new records of the post_process_map table in OMOP CDM for the processed FHIR
   *     Condition resource
   */
  private List<PostProcessMap> setBodySiteLocalizationReference(
      @Nullable List<Pair<String, List<IcdSnomedDomainLookup>>> icdSnomedMapList,
      @Nullable Concept omopConcept,
      Long personId,
      String siteLocalization,
      String conditionLogicId,
      String conditionSourceIdentifier) {

    if (icdSnomedMapList == null && omopConcept == null) {
      return Collections.emptyList();
    }

    ArrayList<PostProcessMap> icdSiteLocalizations = new ArrayList<>();
    if (icdSnomedMapList != null) {
      for (var singlePair : icdSnomedMapList) {
        var icdCode = singlePair.getLeft();
        var icdSnomedMap = singlePair.getRight();

        for (var icdSnomed : icdSnomedMap) {

          icdSiteLocalizations.add(
              PostProcessMap.builder()
                  .type(ResourceType.CONDITION.name())
                  .dataOne(siteLocalization + ":Observation")
                  .dataTwo(icdCode + ":" + icdSnomed.getSnomedDomainId())
                  .omopId(personId)
                  .omopTable("site_localization")
                  .fhirLogicalId(conditionLogicId)
                  .fhirIdentifier(conditionSourceIdentifier)
                  .build());
        }
      }
    } else {
      icdSiteLocalizations.add(
          PostProcessMap.builder()
              .type(ResourceType.CONDITION.name())
              .dataOne(siteLocalization + ":Observation")
              .dataTwo(omopConcept.getConceptCode() + ":" + omopConcept.getDomainId())
              .omopId(personId)
              .omopTable("site_localization")
              .fhirLogicalId(conditionLogicId)
              .fhirIdentifier(conditionSourceIdentifier)
              .build());
    }

    return icdSiteLocalizations;
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
  private List<PostProcessMap> setDiagnoseMetaInfoReference(
      @Nullable List<Pair<String, List<IcdSnomedDomainLookup>>> icdSnomedMapList,
      @Nullable Concept omopConcept,
      Long personId,
      String diagnoseMetaInfo,
      String conditionLogicId,
      String conditionSourceIdentifier,
      String metaInfoType) {

    if (icdSnomedMapList == null && omopConcept == null) {
      return Collections.emptyList();
    }

    ArrayList<PostProcessMap> metaInfos = new ArrayList<>();
    if (icdSnomedMapList != null) {
      for (var singlePair : icdSnomedMapList) {
        var icdCode = singlePair.getLeft();
        var icdSnomedMap = singlePair.getRight();

        for (var icdSnomed : icdSnomedMap) {

          metaInfos.add(
              PostProcessMap.builder()
                  .type(ResourceType.CONDITION.name())
                  .dataOne(diagnoseMetaInfo + ":Observation")
                  .dataTwo(icdCode + ":" + icdSnomed.getSnomedDomainId())
                  .omopId(personId)
                  .omopTable(metaInfoType)
                  .fhirLogicalId(conditionLogicId)
                  .fhirIdentifier(conditionSourceIdentifier)
                  .build());
        }
      }
    } else {
      metaInfos.add(
          PostProcessMap.builder()
              .type(ResourceType.CONDITION.name())
              .dataOne(diagnoseMetaInfo + ":Observation")
              .dataTwo(omopConcept.getConceptCode() + ":" + omopConcept.getDomainId())
              .omopId(personId)
              .omopTable(metaInfoType)
              .fhirLogicalId(conditionLogicId)
              .fhirIdentifier(conditionSourceIdentifier)
              .build());
    }

    return metaInfos;
  }

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
              icdSiteLocalization.getCode(), SOURCE_VOCABULARY_ID_ICD_LOCALIZATION, dbMappings);
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
    var siteLocalizationCodingExtension =
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
    var bodySiteCodeable = srcCondition.getBodySite();
    if (bodySiteCodeable.isEmpty()) {
      return null;
    }

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
