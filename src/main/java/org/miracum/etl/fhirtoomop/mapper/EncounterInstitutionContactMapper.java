package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_INPATIENT;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_PRIMARY_DIAGNOSIS;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_SECONDARY_DIAGNOSIS;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_STILL_PATIENT;
import static org.miracum.etl.fhirtoomop.Constants.MAX_SOURCE_VALUE_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_DIAGNOSIS_TYPE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_VISIT_STATUS;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_VISIT_TYPE;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Encounter.DiagnosisComponent;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Reference;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper.Tablename;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.miracum.etl.fhirtoomop.model.omop.VisitOccurrence;
import org.miracum.etl.fhirtoomop.repository.service.EncounterInstitutionContactMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The EncounterInstitutionContactMapper class describes the business logic of transforming a FHIR
 * Encounter resource (supply case/administrative case) to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class EncounterInstitutionContactMapper implements FhirMapper<Encounter> {

  private final DbMappings dbMappings;
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final ResourceFhirReferenceUtils resourceFhirReferenceUtils;
  private final Boolean bulkload;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired FindOmopConcepts findOmopConcepts;
  @Autowired EncounterInstitutionContactMapperServiceImpl institutionContactService;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("EncounterInstitutionContact");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("EncounterInstitutionContact");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("EncounterInstitutionContact");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("EncounterInstitutionContact");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("EncounterInstitutionContact");

  /**
   * Constructor for objects of the class EncounterInstitutionContactMapper.
   *
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public EncounterInstitutionContactMapper(
      ResourceFhirReferenceUtils referenceUtils, Boolean bulkload, DbMappings dbMappings) {
    this.resourceFhirReferenceUtils = referenceUtils;
    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR Encounter resource (supply case/administrative case) to several OMOP CDM tables.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Encounter
   *     resource
   */
  @Override
  public OmopModelWrapper map(Encounter srcEncounter, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var encounterSourceIdentifier = fhirReferenceUtils.extractIdentifier(srcEncounter, "VN");
    var encounterLogicId = fhirReferenceUtils.extractId(srcEncounter);
    if (Strings.isNullOrEmpty(encounterLogicId)
        && Strings.isNullOrEmpty(encounterSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Encounter] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }
    String encounterId = "";
    if (!Strings.isNullOrEmpty(encounterLogicId)) {
      encounterId = srcEncounter.getId();
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingDiagnosisInformation(encounterLogicId, encounterSourceIdentifier);
      if (isDeleted) {
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", encounterLogicId);
        deleteExistingVisitOccs(encounterLogicId, encounterSourceIdentifier);
        deletedFhirReferenceCounter.increment();
        return null;
      }
    }

    var personId = getPersonId(srcEncounter, encounterLogicId, encounterId);
    if (personId == null) {
      log.warn("No matching [Person] found for {}. Skip resource", encounterLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var institutionContactOnset = getInstitutionContactOnset(srcEncounter);

    if (institutionContactOnset.getStartDateTime() == null) {
      log.warn("No [start_date] found for {}. Skip resource", encounterLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var newVisitOccurrence =
        createNewVisitOccurrence(
            srcEncounter,
            encounterLogicId,
            encounterSourceIdentifier,
            personId,
            institutionContactOnset);

    wrapper.setVisitOccurrence(newVisitOccurrence);

    var diagnosisInformation =
        getDiagnosisInformation(srcEncounter, encounterLogicId, encounterSourceIdentifier);
    if (!diagnosisInformation.isEmpty()) {
      wrapper.setPostProcessMap(diagnosisInformation);
    }

    wrapper
        .getPostProcessMap()
        .add(
            createObservationPeriod(
                newVisitOccurrence.getVisitStartDatetime(),
                newVisitOccurrence.getVisitEndDatetime(),
                personId));

    var admissionOccasion =
        setAdmissionOccasion(
            srcEncounter,
            personId,
            institutionContactOnset.getStartDateTime(),
            encounterLogicId,
            encounterSourceIdentifier);

    wrapper.getPostProcessMap().add(admissionOccasion);

    var admissionReason =
        setAdmissionReason(
            srcEncounter,
            personId,
            institutionContactOnset.getStartDateTime(),
            encounterLogicId,
            encounterSourceIdentifier);
    if (admissionReason != null) {
      wrapper.getPostProcessMap().add(admissionReason);
    }

    var dischargeReason =
        setDischargeReason(
            srcEncounter,
            personId,
            newVisitOccurrence.getVisitEndDatetime(),
            encounterLogicId,
            encounterSourceIdentifier);

    wrapper.getPostProcessMap().add(dischargeReason);

    return wrapper;
  }

  /**
   * Delete FHIR Encounter resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   */
  private void deleteExistingVisitOccs(String encounterLogicId, String encounterSourceIdentifier) {
    if (!Strings.isNullOrEmpty(encounterLogicId)) {
      institutionContactService.deleteVisitOccByLogicalId(encounterLogicId);
    } else {
      institutionContactService.deleteVisitOccByIdentifier(encounterSourceIdentifier);
    }
  }
  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR Encounter
   * resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Encounter srcEncounter, String encounterLogicId, String encounterId) {
    var patientReferenceIdentifier =
        resourceFhirReferenceUtils.getSubjectReferenceIdentifier(srcEncounter);
    var patientReferenceLogicalId =
        resourceFhirReferenceUtils.getSubjectReferenceLogicalId(srcEncounter);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, encounterLogicId, encounterId);
  }

  /**
   * Extracts date time information from the FHIR Encounter resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @return start date time and end date time of the FHIR Encounter resource
   */
  private ResourceOnset getInstitutionContactOnset(Encounter srcEncounter) {
    var resourceOnset = new ResourceOnset();

    if (!srcEncounter.hasPeriod() || srcEncounter.getPeriod() == null) {
      return resourceOnset;
    }

    var encounterPeriod = srcEncounter.getPeriod();
    if (encounterPeriod.getStart() != null) {
      resourceOnset.setStartDateTime(
          new Timestamp(encounterPeriod.getStart().getTime()).toLocalDateTime());
    }

    if (encounterPeriod.getEnd() != null) {
      resourceOnset.setEndDateTime(
          new Timestamp(encounterPeriod.getEnd().getTime()).toLocalDateTime());
    }
    return resourceOnset;
  }

  /**
   * Creates a new record of the visit_occurrence table in OMOP CDM for the processed FHIR Encounter
   * resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param institutionContactOnset start date time and end date time of the FHIR Encounter resource
   * @return new record of the visit_occurrence table in OMOP CDM for the processed FHIR Encounter
   *     resource
   */
  private VisitOccurrence createNewVisitOccurrence(
      Encounter srcEncounter,
      String encounterLogicId,
      String encounterSourceIdentifier,
      Long personId,
      ResourceOnset institutionContactOnset) {
    var startDateTime = institutionContactOnset.getStartDateTime();
    var endDateTime = institutionContactOnset.getEndDateTime();
    var visitTypeConceptId = getVisitTypeConceptId(srcEncounter, endDateTime);
    var visitEndDateTime = setVisitEndDateTime(visitTypeConceptId, endDateTime, encounterLogicId);
    var visitSourceValue = cutString(encounterSourceIdentifier);
    var visitOccurrence =
        VisitOccurrence.builder()
            .visitStartDate(startDateTime.toLocalDate())
            .visitStartDatetime(startDateTime)
            .visitConceptId(getPatientVisitType(srcEncounter))
            .personId(personId)
            .visitTypeConceptId(visitTypeConceptId)
            .fhirLogicalId(encounterLogicId)
            .fhirIdentifier(encounterSourceIdentifier)
            .visitEndDatetime(visitEndDateTime)
            .visitEndDate(visitEndDateTime.toLocalDate())
            .visitSourceValue(visitSourceValue == null ? null : visitSourceValue.substring(4))
            .build();
    if (bulkload.equals(Boolean.FALSE)) {
      var existingVisitOccId =
          omopReferenceUtils.getExistingVisitOccId(encounterLogicId, encounterSourceIdentifier);
      if (existingVisitOccId != null) {
        log.debug(
            "[Encounter] {} exists already in visit_occurrence. Update existing visit_occurrence",
            encounterLogicId);
        visitOccurrence.setVisitOccurrenceId(existingVisitOccId);
      }
    }
    return visitOccurrence;
  }

  /**
   * Searches the visit_type_concept_id in OMOP CDM for the extracted status from the FHIR Encounter
   * resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param endDateTime end date time of the FHIR Encounter resource
   * @return visit_type_concept_id of Encounter status in OMOP CDM
   */
  private Integer getVisitTypeConceptId(Encounter srcEncounter, LocalDateTime endDateTime) {
    if (srcEncounter.hasStatus() && srcEncounter.getStatus() != null) {
      var visitStatus = srcEncounter.getStatus().toString();

      if (visitStatus.equals("UNKNOWN") && endDateTime == null) {
        return CONCEPT_STILL_PATIENT;
      }

      var sourceToConceptMap =
          findOmopConcepts.getCustomConcepts(
              new Coding(null, visitStatus, null), SOURCE_VOCABULARY_ID_VISIT_STATUS, dbMappings);
      var visitTypeConceptId = sourceToConceptMap.getTargetConceptId();
      if (!visitTypeConceptId.equals(CONCEPT_NO_MATCHING_CONCEPT)) {
        return visitTypeConceptId;
      }
    }

    return CONCEPT_EHR;
  }

  /**
   * Searches the visit_concept_id in OMOP CDM for the extracted class from the FHIR Encounter
   * resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @return visit_concept_id of Encounter class in OMOP CDM
   */
  private Integer getPatientVisitType(Encounter srcEncounter) {
    if (!srcEncounter.hasClass_() || Strings.isNullOrEmpty(srcEncounter.getClass_().getCode())) {
      log.debug("No class found for Encounter {}.", srcEncounter.getIdElement().getIdPart());
      return CONCEPT_NO_MATCHING_CONCEPT;
    }
    var visitType = srcEncounter.getClass_().getCode();
    if (visitType.equalsIgnoreCase("station") || visitType.equalsIgnoreCase("stationaer")) {
      return CONCEPT_INPATIENT;
    }
    var sourceToConceptMap =
        findOmopConcepts.getCustomConcepts(
            new Coding(null, visitType, null), SOURCE_VOCABULARY_ID_VISIT_TYPE, dbMappings);
    return sourceToConceptMap.getTargetConceptId();
  }

  /**
   * Sets the end date and time of the new record of the visit_occurrence table in OMOP CDM.
   *
   * @param newVisitOccurrence new record of the visit_occurrence table in OMOP CDM for the
   *     processed FHIR Encounter resource
   * @param endDateTime end date time of the FHIR Encounter resource
   * @param encounterLogicId logical id of FHIR Encounter resource
   */
  private LocalDateTime setVisitEndDateTime(
      Integer visitTypeConceptId, LocalDateTime endDateTime, String encounterLogicId) {
    if (visitTypeConceptId.equals(CONCEPT_STILL_PATIENT)) {
      return LocalDateTime.now();
    } else {
      if (endDateTime != null) {
        return endDateTime;
      } else {
        log.warn(
            "Missing [Enddate] for terminated [Encounter] {}, set default. Please check.",
            encounterLogicId);
        return LocalDateTime.of(LocalDate.now(), LocalTime.MIDNIGHT);
      }
    }
  }

  /**
   * Shortens a string value to a maximum length.
   *
   * @param identifier identifier of the FHIR Encounter resource
   * @return shortened string value
   */
  private String cutString(String identifier) {
    if (StringUtils.isBlank(identifier)) {
      return null;
    }
    if (identifier.length() > MAX_SOURCE_VALUE_LENGTH) {
      log.debug(
          "Truncating overlong encounter source identifier={} to maxSourceValueLength={} characters",
          identifier,
          MAX_SOURCE_VALUE_LENGTH);
      identifier = StringUtils.left(identifier, MAX_SOURCE_VALUE_LENGTH);
    }
    return identifier;
  }

  /**
   * Creates a new record of the post_process_map table in OMOP CDM for admission reason.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param startDateTime start date time of the FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   * @return new record of the post_process_map table in OMOP CDM for the extracted admission reason
   *     from the processed FHIR Encounter resource
   */
  private PostProcessMap setAdmissionReason(
      Encounter srcEncounter,
      Long personId,
      LocalDateTime startDateTime,
      String encounterLogicId,
      String encounterSourceIdentifier) {
    var admissionReasonCode = getAdmissionReasonCode(srcEncounter);
    if (admissionReasonCode != null) {
      return PostProcessMap.builder()
          .type(ResourceType.ENCOUNTER.toString())
          .dataOne(startDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
          .dataTwo(admissionReasonCode)
          .omopId(personId)
          .omopTable("admission_reason")
          .fhirLogicalId(encounterLogicId)
          .fhirIdentifier(encounterSourceIdentifier)
          .build();

    } else {
      log.debug("No [Admission reason] found for [{}].", srcEncounter.getId());
      return null;
    }
  }

  /**
   * Extracts admission reason information from FHIR Encounter resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @return admission reason code from FHIR Encounter resource
   */
  private String getAdmissionReasonCode(Encounter srcEncounter) {

    if (!srcEncounter.hasReasonCode() && srcEncounter.getReasonCode().isEmpty()) {
      return null;
    }

    var admissionReasonCoding =
        srcEncounter.getReasonCode().stream()
            .filter(CodeableConcept::hasCoding)
            .filter(codeable -> !codeable.getCoding().isEmpty())
            .flatMap(codeable -> codeable.getCoding().stream())
            .filter(coding -> fhirSystems.getAdmissionReason().contains(coding.getSystem()))
            .findFirst();

    if (!admissionReasonCoding.isPresent()) {
      return null;
    }
    return admissionReasonCoding.get().getCode();
  }

  /**
   * Creates a new record of the post_process_map table in OMOP CDM for discharge reason.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param dischargeDateTime end date time of the FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   * @return new record of the post_process_map table in OMOP CDM for the extracted discharge reason
   *     from the processed FHIR Encounter resource
   */
  private PostProcessMap setDischargeReason(
      Encounter srcEncounter,
      Long personId,
      LocalDateTime dischargeDateTime,
      String encounterLogicId,
      String encounterSourceIdentifier) {
    var dischargeReasonCode = getDischargeReasonCode(srcEncounter);
    if (dischargeReasonCode != null) {

      return PostProcessMap.builder()
          .type(ResourceType.ENCOUNTER.toString())
          .dataOne(dischargeDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
          .dataTwo(dischargeReasonCode)
          .omopId(personId)
          .omopTable("discharge_reason")
          .fhirLogicalId(encounterLogicId)
          .fhirIdentifier(encounterSourceIdentifier)
          .build();

    } else {
      log.debug("No [Discharge reason] found for [{}].", srcEncounter.getId());
      return null;
    }
  }

  /**
   * Extracts discharge reason information from FHIR Encounter resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @return discharge reason code from FHIR Encounter resource
   */
  private String getDischargeReasonCode(Encounter srcEncounter) {

    if (!srcEncounter.hasHospitalization()) {

      return null;
    }
    if (!srcEncounter.getHospitalization().hasDischargeDisposition()
        || srcEncounter.getHospitalization().getDischargeDisposition() == null) {
      return null;
    }

    var dischargeReasonCoding =
        srcEncounter.getHospitalization().getDischargeDisposition().getCoding().stream()
            .filter(coding -> fhirSystems.getDischargeReason().contains(coding.getSystem()))
            .findFirst();

    if (!dischargeReasonCoding.isPresent()) {
      return null;
    }
    return dischargeReasonCoding.get().getCode();
  }

  /**
   * Creates a new record of the post_process_map table in OMOP CDM for admission occasion.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param startDateTime start date time of the FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   * @return new record of the post_process_map table in OMOP CDM for the extracted admission
   *     occasion from the processed FHIR Encounter resource
   */
  private PostProcessMap setAdmissionOccasion(
      Encounter srcEncounter,
      Long personId,
      LocalDateTime startDateTime,
      String encounterLogicId,
      String encounterSourceIdentifier) {

    var admissionOccasionCode = getAdmissionOccasionCode(srcEncounter);
    if (admissionOccasionCode != null) {
      return PostProcessMap.builder()
          .type(ResourceType.ENCOUNTER.toString())
          .dataOne(startDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
          .dataTwo(admissionOccasionCode)
          .omopId(personId)
          .omopTable("admission_occasion")
          .fhirLogicalId(encounterLogicId)
          .fhirIdentifier(encounterSourceIdentifier)
          .build();
    } else {
      log.debug("No [Admission occasion] found for [{}].", encounterLogicId);
      return null;
    }
  }

  /**
   * Extracts admission occasion information from FHIR Encounter resource.
   *
   * @param srcEncounter FHIR Encounter resource
   * @return admission occasion code from FHIR Encounter resource
   */
  private String getAdmissionOccasionCode(Encounter srcEncounter) {
    if (!srcEncounter.hasHospitalization()) {

      return null;
    }
    if (!srcEncounter.getHospitalization().hasAdmitSource()
        || srcEncounter.getHospitalization().getAdmitSource() == null) {
      return null;
    }

    var admissionOccasionCoding =
        srcEncounter.getHospitalization().getAdmitSource().getCoding().stream()
            .filter(coding -> fhirSystems.getAdmissionOccasion().contains(coding.getSystem()))
            .findFirst();

    if (!admissionOccasionCoding.isPresent()) {
      return null;
    }
    return admissionOccasionCoding.get().getCode();
  }

  /**
   * Extracts diagnosis rank information from FHIR Encounter resource and create a new record of the
   * post_process_map table in OMOP CDM for diagnosis rank information.
   *
   * @param diagnosis DiagnosisComponent of the FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   * @param conditionReference logical id of the referenced FHIR Condition resource
   * @param conditionIdentifier identifier of the referenced FHIR Condition resource
   * @return new record of the post_process_map table in OMOP CDM for diagnosis rank information
   *     from the processed FHIR Encounter resource
   */
  private PostProcessMap getRankings(
      DiagnosisComponent diagnosis,
      String encounterLogicId,
      String encounterSourceIdentifier,
      String conditionReference,
      String conditionIdentifier) {

    if (diagnosis.hasRank() && diagnosis.getRank() > 0) {
      var rank = diagnosis.getRank();
      var rankConceptId =
          Long.valueOf(rank == 1 ? CONCEPT_PRIMARY_DIAGNOSIS : CONCEPT_SECONDARY_DIAGNOSIS);

      return PostProcessMap.builder()
          .dataOne(conditionReference + ":" + conditionIdentifier)
          .dataTwo(rank + ":" + String.valueOf(rankConceptId))
          .type(ResourceType.ENCOUNTER.toString())
          .omopId(0L)
          .omopTable("rank")
          .fhirLogicalId(encounterLogicId)
          .fhirIdentifier(encounterSourceIdentifier)
          .build();
    }

    return null;
  }

  /**
   * Extracts diagnosis use information from FHIR Encounter resource and create a new record of the
   * post_process_map table in OMOP CDM for diagnosis use information.
   *
   * @param diagnosis DiagnosisComponent of the FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   * @param conditionReference logical id of the referenced FHIR Condition resource
   * @param conditionIdentifier identifier of the referenced FHIR Condition resource
   * @return new record of the post_process_map table in OMOP CDM for diagnosis use information from
   *     the processed FHIR Encounter resource
   */
  private PostProcessMap getDiagnosisType(
      DiagnosisComponent diagnosis,
      String encounterLogicId,
      String encounterSourceIdentifier,
      String conditionReference,
      String conditionIdentifier) {

    if (diagnosis.hasUse() && !diagnosis.getUse().isEmpty()) {
      var diagnosisUse = diagnosis.getUse().getCoding();
      if (diagnosisUse.isEmpty()) {
        return null;
      }

      var useCodingOpt =
          diagnosisUse.stream()
              .filter(coding -> coding.getSystem().equals(fhirSystems.getDiagnosisUse()))
              .findFirst();
      if (!useCodingOpt.isPresent()) {
        return null;
      }
      var useCoding = useCodingOpt.get();
      if (useCoding == null) {
        return null;
      }

      if (!Strings.isNullOrEmpty(useCoding.getCode())) {
        var type = useCoding.getCode();

        var typeConceptId =
            findOmopConcepts
                .getCustomConcepts(useCoding, SOURCE_VOCABULARY_ID_DIAGNOSIS_TYPE, dbMappings)
                .getTargetConceptId();
        if (typeConceptId != null) {

          return PostProcessMap.builder()
              .dataOne(conditionReference + ":" + conditionIdentifier)
              .dataTwo(type + ":" + String.valueOf(typeConceptId))
              .type(ResourceType.ENCOUNTER.toString())
              .omopId(0L)
              .omopTable("use")
              .fhirLogicalId(encounterLogicId)
              .fhirIdentifier(encounterSourceIdentifier)
              .build();
        }
      }
    }

    return null;
  }

  /**
   * Extracts diagnosis information from FHIR Encounter resource and creates new records of the
   * post_process_map table in OMOP CDM for diagnosis use and rank information.
   *
   * @param srcEncounter FHIR Encounter resource
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   */
  private List<PostProcessMap> getDiagnosisInformation(
      Encounter srcEncounter, String encounterLogicId, String encounterSourceIdentifier) {
    List<PostProcessMap> postProcessMap = new ArrayList<>();
    List<String> diagnosisReferenceList = new ArrayList<>();
    if (srcEncounter.hasDiagnosis() && !srcEncounter.getDiagnosis().isEmpty()) {

      var diagnoses =
          srcEncounter.getDiagnosis().stream()
              .filter(Objects::nonNull)
              .filter(DiagnosisComponent::hasCondition)
              .collect(Collectors.toList());

      for (var diagnosis : diagnoses) {

        var conditionReference = getConditionReference(diagnosis.getCondition());
        var conditionIdentifier = getConditionIdentifier(diagnosis.getCondition());
        if (Strings.isNullOrEmpty(conditionReference)
            && Strings.isNullOrEmpty(conditionIdentifier)) {
          continue;
        }

        conditionReference = conditionReference == null ? "con-" : conditionReference;
        conditionIdentifier = conditionIdentifier == null ? "con-" : conditionIdentifier;
        diagnosisReferenceList.add(conditionReference);

        var type =
            getDiagnosisType(
                diagnosis,
                encounterLogicId,
                encounterSourceIdentifier,
                conditionReference,
                conditionIdentifier);
        var rank =
            getRankings(
                diagnosis,
                encounterLogicId,
                encounterSourceIdentifier,
                conditionReference,
                conditionIdentifier);
        if (type == null && rank == null) {
          return Collections.emptyList();
        }

        postProcessMap.add(type);
        postProcessMap.add(rank);
      }

      updatePrimarySecondaryInformation(diagnosisReferenceList);
      return postProcessMap;
    }
    return Collections.emptyList();
  }

  /**
   * Extracts the reference from a diagnosis reference.
   *
   * @param diagnosis reference of a condition FHIR resource
   * @return the reference of a condition FHIR resource
   */
  private String getConditionReference(Reference diagnosis) {
    if (diagnosis.hasReferenceElement() && diagnosis.getReferenceElement().hasIdPart()) {
      return "con-" + diagnosis.getReferenceElement().getIdPart();
    }

    return null;
  }

  /**
   * Extracts the identifier from a diagnosis reference.
   *
   * @param diagnosis reference of a condition FHIR resource
   * @return the identifier of a condition FHIR resource
   */
  private String getConditionIdentifier(Reference diagnosis) {
    if (diagnosis.hasIdentifier() && diagnosis.getIdentifier().hasValue()) {
      return "con-" + diagnosis.getIdentifier().getValue();
    }

    return null;
  }

  /**
   * Creates a new record of the post_process_map table in OMOP CDM for observation period
   * information.
   *
   * @param visitStarDateTime start date time of the FHIR Encounter resource
   * @param visitEndDateTime end date time of the FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @return new record of the post_process_map table in OMOP CDM for observation period information
   *     from the processed FHIR Encounter resource
   */
  private PostProcessMap createObservationPeriod(
      LocalDateTime visitStarDateTime, LocalDateTime visitEndDateTime, Long personId) {

    return PostProcessMap.builder()
        .type(ResourceType.ENCOUNTER.name())
        .omopTable(Tablename.OBSERVATIONPERIOD.getTableName())
        .dataOne(visitStarDateTime.toString())
        .dataTwo(visitEndDateTime.toString())
        .omopId(personId)
        .build();
  }

  /**
   * Deletes diagnosis information from post_process_map using fhir_logical_id and fhir_identifier.
   *
   * @param encounterLogicId logical id of the FHIR Encounter resource
   * @param encounterSourceIdentifier identifier of the FHIR Encounter resource
   */
  private void deleteExistingDiagnosisInformation(
      String encounterLogicId, String encounterSourceIdentifier) {
    if (!Strings.isNullOrEmpty(encounterLogicId)) {
      institutionContactService.deleteExistingEncounterByFhirLogicalId(encounterLogicId);
    } else {
      institutionContactService.deleteExistingEncounterByFhirIdentifier(encounterSourceIdentifier);
    }
  }

  /**
   * Deletes primary secondary ICD information from fact_relationship and updates flag for primary
   * secondary ICD information in post_process_map using fhir_logical_ids of the referenced FHIR
   * Condition resources
   *
   * @param diagnosisReferenceList list of logical ids of the referenced FHIR Condition resources
   */
  private void updatePrimarySecondaryInformation(List<String> diagnosisReferenceList) {
    institutionContactService.updatePrimarySecondaryInformation(diagnosisReferenceList);
  }
}
