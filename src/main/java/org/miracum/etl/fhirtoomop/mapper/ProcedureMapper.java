package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_PROCEDURE_BODYSITE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_PROCEDURE_DICOM;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_OPS;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;

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
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Procedure;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.DeviceExposure;
import org.miracum.etl.fhirtoomop.model.omop.ProcedureOccurrence;
import org.miracum.etl.fhirtoomop.repository.service.DeviceExposureMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.ProcedureMapperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The ProcedureMapper class describes the business logic of transforming a FHIR Procedure resource
 * to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class ProcedureMapper implements FhirMapper<Procedure> {

  private static final FhirSystems fhirSystems = new FhirSystems();

  private final Boolean bulkload;
  private final DbMappings dbMappings;
  private final List<String> listOfProcedureVocabularyId =
      Arrays.asList(SOURCE_VOCABULARY_ID_PROCEDURE_DICOM, VOCABULARY_OPS, VOCABULARY_SNOMED);

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessProcedures");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessProcedures");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessProcedures");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessProcedures");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessProcedures");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessProcedures");

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ProcedureMapperServiceImpl procedureService;
  @Autowired DeviceExposureMapperServiceImpl deviceExposureService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;

  /**
   * Constructor for objects of the class ProcedureMapper.
   *
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public ProcedureMapper(Boolean bulkload, DbMappings dbMappings) {
    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR Procedure resource to procedure_occurrence table in OMOP CDM.
   *
   * @param srcProcedure FHIR Procedure resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Procedure
   *     resource
   */
  @Override
  public OmopModelWrapper map(Procedure srcProcedure, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var procedureLogicId = fhirReferenceUtils.extractId(srcProcedure);

    var procedureSourceIdentifier = fhirReferenceUtils.extractResourceFirstIdentifier(srcProcedure);
    if (Strings.isNullOrEmpty(procedureLogicId)
        && Strings.isNullOrEmpty(procedureSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Procedure] resource is invalid. Skip resource");

      noFhirReferenceCounter.increment();
      return null;
    }

    var statusElement = srcProcedure.getStatusElement();
    var statusValue = checkDataAbsentReason.getValue(statusElement);
    if (Strings.isNullOrEmpty(statusValue)
        || !FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status] of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          procedureLogicId);
      return null;
    }

    if (bulkload.equals(Boolean.FALSE)) {

      deleteExistingProcedureOccurrence(procedureLogicId, procedureSourceIdentifier);
      deleteExistingDeviceExposure(procedureLogicId, procedureSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", procedureLogicId);
        return null;
      }
    }

    var personId = getPersonId(srcProcedure, procedureLogicId);
    if (personId == null) {
      log.warn("No matching [Person] found for {}. Skip resource", procedureLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var procedureCodings = getProcedureCodings(srcProcedure, procedureLogicId);
    if (procedureCodings.isEmpty()) {
      log.warn("No Code found in [Procedure]:{}. Skip resource", procedureLogicId);
      noCodeCounter.increment();
      return null;
    }

    var procedureOnset = getProcedureOnset(srcProcedure);
    if (procedureOnset.getStartDateTime() == null) {
      log.warn("Unable to determine [Performed DateTime] for {}. Skip resource", procedureLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcProcedure, personId, procedureLogicId);

    var newProcedureOccurrence =
        createProcedureOccurrence(
            procedureCodings,
            procedureOnset.getStartDateTime(),
            personId,
            visitOccId,
            procedureLogicId,
            procedureSourceIdentifier,
            srcProcedure);

    wrapper.getProcedureOccurrence().addAll(newProcedureOccurrence);
    var usedCodesCodeableConcepts = srcProcedure.getUsedCode();
    if (!usedCodesCodeableConcepts.isEmpty()) {
      var deviceExposures =
          createDeviceExposure(
              personId,
              visitOccId,
              procedureOnset,
              procedureLogicId,
              procedureSourceIdentifier,
              usedCodesCodeableConcepts);
      wrapper.setDeviceExposure(deviceExposures);
    }

    return wrapper;
  }

  private List<Coding> extractDeviceCode(List<CodeableConcept> usedCodesCodeableConcepts) {

    List<Coding> devicesCodes = new ArrayList<>();
    for (var usedCodesCodeableConcept : usedCodesCodeableConcepts) {
      var usedCodesCodings = usedCodesCodeableConcept.getCoding();
      if (usedCodesCodings.isEmpty()) {

        continue;
      }
      usedCodesCodings.forEach(devicesCodes::add);
    }
    return devicesCodes;
  }

  private List<DeviceExposure> createDeviceExposure(
      Long personId,
      Long visitOccId,
      ResourceOnset procedureOnset,
      String procedureLogicId,
      String procedureSourceIdentifier,
      List<CodeableConcept> usedCodesCodeableConcepts) {
    var deviceCodings = extractDeviceCode(usedCodesCodeableConcepts);
    if (deviceCodings.isEmpty()) {

      return Collections.emptyList();
    }
    List<DeviceExposure> deviceExposures = new ArrayList<>();
    for (var deviceCoding : deviceCodings) {
      var deviceCode = checkDataAbsentReason.getValue(deviceCoding.getCodeElement());
      if (Strings.isNullOrEmpty(deviceCode)) {
        continue;
      }

      var startDateTime = procedureOnset.getStartDateTime();
      var endDateTime = procedureOnset.getEndDateTime();
      var deviceConcept =
          findOmopConcepts.getConcepts(
              deviceCoding, startDateTime.toLocalDate(), bulkload, dbMappings);
      if (deviceConcept == null) {
        continue;
      }
      var deviceExposure =
          DeviceExposure.builder()
              .personId(personId)
              .visitOccurrenceId(visitOccId)
              .fhirIdentifier(procedureSourceIdentifier)
              .fhirLogicalId(procedureLogicId)
              .deviceExposureStartDate(startDateTime.toLocalDate())
              .deviceExposureStartDatetime(startDateTime)
              .deviceExposureEndDate(endDateTime == null ? null : endDateTime.toLocalDate())
              .deviceExposureEndDatetime(endDateTime)
              .deviceTypeConceptId(CONCEPT_EHR)
              .deviceConceptId(deviceConcept.getConceptId())
              .deviceSourceConceptId(deviceConcept.getConceptId())
              .deviceSourceValue(deviceCode)
              .build();

      addToList(deviceExposures, deviceExposure);
    }
    return deviceExposures;
  }

  private void addToList(List<DeviceExposure> deviceExposures, DeviceExposure deviceExposure) {
    if (deviceExposures.isEmpty()) {
      deviceExposures.add(deviceExposure);
      return;
    }
    if (deviceExposures.contains(deviceExposure)) {
      return;
    }
    deviceExposures.add(deviceExposure);
  }

  /**
   * Creates a new record of the procedure_occurrence table in OMOP CDM for the processed FHIR
   * Procedure resource.
   *
   * @param procedureCodings a list of Coding elements from Procedure FHIR resource
   * @param procedureStartDatetime date time of the FHIR Procedure resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @param procedureSourceIdentifier identifier of the FHIR Procedure resource
   * @param bodySite anatomical body site information from procedure FHIR resource
   * @return new record of the procedure_occurrence table in OMOP CDM for the processed FHIR
   *     Procedure resource
   */
  private List<ProcedureOccurrence> createProcedureOccurrence(
      List<Coding> procedureCodings,
      LocalDateTime procedureStartDatetime,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      Procedure srcProcedure) {

    //    ProcedureOccurrence procedureOccurrence =
    //        createBasisProcedureOccurrence(
    //            procedureStartDatetime,
    //            personId,
    //            visitOccId,
    //            procedureLogicId,
    //            procedureSourceIdentifier);

    List<ProcedureOccurrence> procedureList = new ArrayList<>();
    var codingSize = procedureCodings.size();
    if (codingSize == 1) {
      setProcedureConceptsUsingSingleCoding(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureList);
    } else {
      setProcedureConceptsUsingMultipleCodings(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureList);
    }

    if (procedureList.isEmpty()) {
      return Collections.emptyList();
    }

    return procedureList;
  }

  /**
   * @param procedureStartDatetime
   * @param personId
   * @param visitOccId
   * @param procedureLogicId
   * @param procedureSourceIdentifier
   * @return
   */
  private ProcedureOccurrence createBasisProcedureOccurrence(
      LocalDateTime procedureStartDatetime,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier) {
    return ProcedureOccurrence.builder()
        .procedureDate(procedureStartDatetime.toLocalDate())
        .procedureDatetime(procedureStartDatetime)
        .personId(personId)
        .visitOccurrenceId(visitOccId)
        .procedureTypeConceptId(CONCEPT_EHR)
        .fhirLogicalId(procedureLogicId)
        .fhirIdentifier(procedureSourceIdentifier)
        .build();
  }

  /**
   * @param procedureStartDatetime
   * @param personId
   * @param visitOccId
   * @param procedureLogicId
   * @param procedureSourceIdentifier
   * @param procedureCoding
   * @return
   */
  private void setProcedureConceptsUsingSingleCoding(
      LocalDateTime procedureStartDatetime,
      List<Coding> procedureCodings,
      Procedure srcProcedure,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      List<ProcedureOccurrence> procedureList) {

    for (var procedureCoding : procedureCodings) {

      var procedureCodeExist =
          checkIfAnyProcedureCodesExist(procedureCoding, listOfProcedureVocabularyId);
      if (!procedureCodeExist) {
        continue;
      }
      var procedureOccurrence =
          createBasisProcedureOccurrence(
              procedureStartDatetime,
              personId,
              visitOccId,
              procedureLogicId,
              procedureSourceIdentifier);
      var procedureVocabularyId = findOmopConcepts.getOmopVocabularyId(procedureCoding.getSystem());
      if (procedureVocabularyId.equals(SOURCE_VOCABULARY_ID_PROCEDURE_DICOM)) {
        var procedureConcept =
            findOmopConcepts.getCustomConcepts(
                procedureCoding.getCode(), procedureVocabularyId, dbMappings);
        if (procedureConcept == null) {
          continue;
        }
        procedureOccurrence.setProcedureSourceValue(procedureCoding.getCode());
        procedureOccurrence.setProcedureConceptId(procedureConcept.getTargetConceptId());
        procedureOccurrence.setProcedureSourceConceptId(procedureConcept.getTargetConceptId());

      } else {
        var procedureConcept =
            findOmopConcepts.getConcepts(
                procedureCoding, procedureStartDatetime.toLocalDate(), bulkload, dbMappings);
        if (procedureConcept == null) {
          continue;
        }
        procedureOccurrence.setProcedureSourceValue(procedureCoding.getCode());
        procedureOccurrence.setProcedureConceptId(procedureConcept.getConceptId());
        procedureOccurrence.setProcedureSourceConceptId(procedureConcept.getConceptId());
      }

      var procedureBodySiteLocalization =
          getBodySiteLocalization(
              srcProcedure, procedureCoding, procedureStartDatetime.toLocalDate());
      setProcedureModifier(procedureOccurrence, procedureBodySiteLocalization);
      procedureList.add(procedureOccurrence);
    }
  }

  /**
   * @param procedureStartDatetime
   * @param personId
   * @param visitOccId
   * @param procedureLogicId
   * @param procedureSourceIdentifier
   * @param opsCoding
   * @param snomedCoding
   * @return
   */
  private void setProcedureConceptsUsingMultipleCodings(
      LocalDateTime procedureStartDatetime,
      List<Coding> procedureCodings,
      Procedure srcProcedure,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      List<ProcedureOccurrence> procedureList) {

    var opsCoding = getCoding(procedureCodings, VOCABULARY_OPS);
    var snomedCoding = getCoding(procedureCodings, VOCABULARY_SNOMED);
    var dicomCoding = getCoding(procedureCodings, SOURCE_VOCABULARY_ID_PROCEDURE_DICOM);

    var opsCodeExist = checkIfSpecificProcedureCodesExist(opsCoding, VOCABULARY_OPS);
    var snomedCodeExist = checkIfSpecificProcedureCodesExist(snomedCoding, VOCABULARY_SNOMED);
    var dicomCodeExist =
        checkIfSpecificProcedureCodesExist(dicomCoding, SOURCE_VOCABULARY_ID_PROCEDURE_DICOM);

    if (snomedCodeExist && opsCodeExist && dicomCodeExist) {

      setProcedureUsingSnomedAndOps(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          opsCoding,
          snomedCoding,
          procedureList);
      setProcedureUsingSnomedAndDicom(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          snomedCoding,
          dicomCoding,
          procedureList);
    } else if (snomedCodeExist && opsCodeExist) {
      setProcedureUsingSnomedAndOps(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          opsCoding,
          snomedCoding,
          procedureList);

    } else if (snomedCodeExist && dicomCodeExist) {
      setProcedureUsingSnomedAndDicom(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          snomedCoding,
          dicomCoding,
          procedureList);

    } else {
      setProcedureConceptsUsingSingleCoding(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureList);
    }
  }

  /**
   * @param procedureStartDatetime
   * @param procedureCodings
   * @param srcProcedure
   * @param procedureOccurrence
   * @param snomedCoding
   * @param dicomCoding
   */
  private void setProcedureUsingSnomedAndDicom(
      LocalDateTime procedureStartDatetime,
      List<Coding> procedureCodings,
      Procedure srcProcedure,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      Coding snomedCoding,
      Coding dicomCoding,
      List<ProcedureOccurrence> procedureList) {
    var dicomConcept =
        findOmopConcepts.getCustomConcepts(
            dicomCoding.getCode(), SOURCE_VOCABULARY_ID_PROCEDURE_DICOM, dbMappings);

    var snomedConcept =
        findOmopConcepts.getConcepts(
            snomedCoding, procedureStartDatetime.toLocalDate(), bulkload, dbMappings);

    if (dicomConcept == null && snomedConcept == null) {
      return;
    }
    var procedureOccurrence =
        createBasisProcedureOccurrence(
            procedureStartDatetime,
            personId,
            visitOccId,
            procedureLogicId,
            procedureSourceIdentifier);
    if (dicomConcept != null && snomedConcept != null) {
      procedureOccurrence.setProcedureSourceValue(dicomCoding.getCode());
      procedureOccurrence.setProcedureConceptId(snomedConcept.getConceptId());
      procedureOccurrence.setProcedureSourceConceptId(dicomConcept.getTargetConceptId());

      var procedureBodySiteLocalization =
          getBodySiteLocalization(srcProcedure, null, procedureStartDatetime.toLocalDate());
      setProcedureModifier(procedureOccurrence, procedureBodySiteLocalization);
      procedureList.add(procedureOccurrence);
    } else {
      setProcedureConceptsUsingSingleCoding(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureList);
    }
  }

  /**
   * @param procedureStartDatetime
   * @param procedureCodings
   * @param srcProcedure
   * @param procedureOccurrence
   * @param opsCoding
   * @param snomedCoding
   */
  private void setProcedureUsingSnomedAndOps(
      LocalDateTime procedureStartDatetime,
      List<Coding> procedureCodings,
      Procedure srcProcedure,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      Coding opsCoding,
      Coding snomedCoding,
      List<ProcedureOccurrence> procedureList) {
    var opsConcept =
        findOmopConcepts.getConcepts(
            opsCoding, procedureStartDatetime.toLocalDate(), bulkload, dbMappings);

    var snomedConcept =
        findOmopConcepts.getConcepts(
            snomedCoding, procedureStartDatetime.toLocalDate(), bulkload, dbMappings);

    if (opsConcept == null && snomedConcept == null) {
      return;
    }

    var procedureOccurrence =
        createBasisProcedureOccurrence(
            procedureStartDatetime,
            personId,
            visitOccId,
            procedureLogicId,
            procedureSourceIdentifier);
    if (opsConcept != null && snomedConcept != null) {
      procedureOccurrence.setProcedureSourceValue(opsCoding.getCode());
      procedureOccurrence.setProcedureConceptId(snomedConcept.getConceptId());
      procedureOccurrence.setProcedureSourceConceptId(opsConcept.getConceptId());
      var procedureBodySiteLocalization =
          getBodySiteLocalization(srcProcedure, opsCoding, procedureStartDatetime.toLocalDate());
      setProcedureModifier(procedureOccurrence, procedureBodySiteLocalization);
      procedureList.add(procedureOccurrence);
    } else {
      setProcedureConceptsUsingSingleCoding(
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureList);
    }
  }

  /**
   * Set procedure_occurrence modifier information.
   *
   * @param procedureOccurrence the new record of the procedure_occurrence
   * @param procedureLocalization Localization coding from the FHIR Procedure resource
   * @param procedureEffective date time of the FHIR Procedure resource
   */
  public void setProcedureModifier(
      ProcedureOccurrence procedureOccurrence,
      Pair<String, Integer> procedureBodySiteLocalization) {

    if (procedureBodySiteLocalization == null) {
      return;
    }
    procedureOccurrence.setModifierSourceValue(procedureBodySiteLocalization.getLeft());
    procedureOccurrence.setModifierConceptId(procedureBodySiteLocalization.getRight());
  }

  /**
   * Delete FHIR Procedure resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @param procedureSourceIdentifier identifier of the FHIR Procedure resource
   */
  private void deleteExistingProcedureOccurrence(
      String procedureLogicId, String procedureSourceIdentifier) {
    if (!Strings.isNullOrEmpty(procedureLogicId)) {
      procedureService.deleteExistingProcedureOccByFhirLogicalId(procedureLogicId);
    } else {
      procedureService.deleteExistingProcedureOccByFhirIdentifier(procedureSourceIdentifier);
    }
  }

  /**
   * Delete FHIR Procedure resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @param procedureSourceIdentifier identifier of the FHIR Procedure resource
   */
  private void deleteExistingDeviceExposure(
      String procedureLogicId, String procedureSourceIdentifier) {
    if (!Strings.isNullOrEmpty(procedureLogicId)) {
      deviceExposureService.deleteExistingDeviceExposureByFhirLogicalId(procedureLogicId);
    } else {
      deviceExposureService.deleteExistingDeviceExposureByFhirIdentifier(procedureSourceIdentifier);
    }
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR Procedure
   * resource.
   *
   * @param srcProcedure FHIR Procedure resource
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Procedure srcProcedure, String procedureLogicId) {
    var patientReferenceIdentifier = fhirReferenceUtils.getSubjectReferenceIdentifier(srcProcedure);
    var patientReferenceLogicalId = fhirReferenceUtils.getSubjectReferenceLogicalId(srcProcedure);
    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, procedureLogicId);
  }

  /**
   * Returns the visit_occurrence_id of the referenced FHIR Encounter resource for the processed
   * FHIR Procedure resource.
   *
   * @param srcProcedure FHIR Procedure resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(Procedure srcProcedure, Long personId, String procedureLogicId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcProcedure);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcProcedure);

    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            srcProcedure.getId());

    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for {}.", procedureLogicId);
    }

    return visitOccId;
  }

  /**
   * Extracts date time information from the FHIR Procedure resource.
   *
   * @param srcProcedure FHIR Procedure resource
   * @return date time of the FHIR Procedure resource
   */
  private ResourceOnset getProcedureOnset(Procedure srcProcedure) {
    var resourceOnset = new ResourceOnset();

    if (srcProcedure.hasPerformedDateTimeType()
        && !srcProcedure.getPerformedDateTimeType().isEmpty()) {

      var performedDateTimeType = srcProcedure.getPerformedDateTimeType();
      var performedDateTime = checkDataAbsentReason.getValue(performedDateTimeType);
      if (performedDateTime != null) {
        resourceOnset.setStartDateTime(performedDateTime);
        return resourceOnset;
      }
    }

    if (srcProcedure.hasPerformedPeriod() && !srcProcedure.getPerformedPeriod().isEmpty()) {
      var performedPeriodElement = srcProcedure.getPerformedPeriod();

      var performedPeriod = checkDataAbsentReason.getValue(performedPeriodElement);
      if (performedPeriod == null) {
        return resourceOnset;
      }

      if (!performedPeriod.getStartElement().isEmpty()) {
        resourceOnset.setStartDateTime(
            new Timestamp(performedPeriod.getStartElement().getValue().getTime())
                .toLocalDateTime());
      }
      if (!performedPeriod.getEndElement().isEmpty()) {
        resourceOnset.setEndDateTime(
            new Timestamp(performedPeriod.getEndElement().getValue().getTime()).toLocalDateTime());
      }
    }
    return resourceOnset;
  }

  /**
   * Extracts the procedure codings from the FHIR Procedure resource as a list.
   *
   * @param srcProcedure FHIR Procedure resource
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @return a list of procedure codings from the FHIR Procedure resource
   */
  private List<Coding> getProcedureCodings(Procedure srcProcedure, String procedureLogicId) {
    List<Coding> codingList = new ArrayList<>();
    var procedureCodings = srcProcedure.getCode().getCoding();
    if (procedureCodings.size() == 1) {
      var procedureCoding = procedureCodings.get(0);
      if (checkIfCodeExist(procedureCoding)) {
        codingList.add(procedureCodings.get(0));
        return codingList;
      }
      return Collections.emptyList();
    }

    if (procedureCodings.size() > 1) {
      log.debug("Found more than one Terminology in [Procedure]:[{}].", procedureLogicId);
      for (var coding : procedureCodings) {
        if (checkIfCodeExist(coding)) {
          codingList.add(coding);
        }
      }
      return codingList;
    }
    return Collections.emptyList();
  }

  /**
   * Extract Coding from Procedure FHIR resource based on the vocabulary ID in OMOP.
   *
   * @param procedureCodings a list of Coding elements from Procedure FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return a Coding from Procedure FHIR
   */
  private Coding getCoding(List<Coding> procedureCodings, String vocabularyId) {
    var codingOptional =
        procedureCodings.stream()
            .filter(
                procedureCoding ->
                    findOmopConcepts
                        .getOmopVocabularyId(procedureCoding.getSystem())
                        .equals(vocabularyId))
            .findFirst();
    if (codingOptional.isPresent()) {
      return codingOptional.get();
    }
    return null;
  }

  private Pair<String, Integer> getBodySiteLocalization(
      Procedure srcProcedure, Coding procedureCoding, LocalDate procedureDate) {
    var siteLocalization = getOpsSiteLocalization(procedureCoding);
    var procedureBodySite = getBodySite(srcProcedure);
    if (siteLocalization == null && procedureBodySite == null) {
      return null;
    } else if (siteLocalization != null) {
      var opsSiteLocalizationConcept =
          findOmopConcepts.getCustomConcepts(
              siteLocalization.getCode(), SOURCE_VOCABULARY_ID_PROCEDURE_BODYSITE, dbMappings);
      return Pair.of(siteLocalization.getCode(), opsSiteLocalizationConcept.getTargetConceptId());
    }
    var procedureBodySiteConcept =
        findOmopConcepts.getConcepts(procedureBodySite, procedureDate, bulkload, dbMappings);
    if (procedureBodySiteConcept == null) {
      return null;
    }
    return Pair.of(procedureBodySite.getCode(), procedureBodySiteConcept.getConceptId());
  }

  /**
   * Extracts OPS site localization information from the FHIR Procedure resource.
   *
   * @param opsCoding OPS coding from the FHIR Procedure resource
   * @return OPS site localization from FHIR Procedure resource
   */
  private Coding getOpsSiteLocalization(Coding opsCoding) {
    if (opsCoding == null) {
      return null;
    }
    var opsSiteLocalizationExtension =
        opsCoding.getExtensionByUrl(fhirSystems.getSiteLocalizationExtension());
    if (opsSiteLocalizationExtension == null) {
      return null;
    }
    var opsSiteLocalizationType = opsSiteLocalizationExtension.getValue();
    if (opsSiteLocalizationType == null) {
      return null;
    }

    var opsSiteLocalizationCoding = opsSiteLocalizationType.castToCoding(opsSiteLocalizationType);
    var opsSiteLocalizationCode = opsSiteLocalizationCoding.getCode();
    if (Strings.isNullOrEmpty(opsSiteLocalizationCode)) {
      return null;
    }
    return opsSiteLocalizationCoding;
  }

  /**
   * Extracts body site information from the FHIR Procedure resource.
   *
   * @param srcProcedure FHIR Procedure resource
   * @return body site from FHIR Procedure resource
   */
  private Coding getBodySite(Procedure srcProcedure) {

    if (!srcProcedure.hasBodySite() || srcProcedure.getBodySite().isEmpty()) {
      return null;
    }

    var bodySiteCodingOptional =
        srcProcedure.getBodySite().get(0).getCoding().stream()
            .filter(code -> code.getSystem().equalsIgnoreCase(fhirSystems.getSnomed()))
            .findAny();
    if (!bodySiteCodingOptional.isPresent()) {
      return null;
    }

    var bodySiteCoding = bodySiteCodingOptional.get();
    var bodySiteCode = bodySiteCoding.getCode();
    if (Strings.isNullOrEmpty(bodySiteCode)) {
      return null;
    }
    return bodySiteCoding;
  }

  /**
   * Check if the used procedure code exists in FHIR Procedure resource
   *
   * @param procedureCoding procedure codings from the FHIR Procedure resource
   * @return a boolean value
   */
  private boolean checkIfCodeExist(Coding procedureCoding) {
    var codeElement = procedureCoding.getCodeElement();
    if (codeElement.isEmpty()) {
      return false;
    }
    var procedureCode = checkDataAbsentReason.getValue(codeElement);
    if (Strings.isNullOrEmpty(procedureCode)) {
      return false;
    }

    return true;
  }

  /**
   * Check if the used procedure code exists in OMOP
   *
   * @param procedureCoding Coding element from Procedure FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return a boolean value
   */
  private boolean checkIfAnyProcedureCodesExist(Coding procedureCoding, List<String> vocabularyId) {
    if (procedureCoding == null) {
      return false;
    }
    var codingVocabularyId = findOmopConcepts.getOmopVocabularyId(procedureCoding.getSystem());
    //    return codingVocabularyId.equals(vocabularyId);
    return vocabularyId.contains(codingVocabularyId);
  }

  private boolean checkIfSpecificProcedureCodesExist(Coding procedureCoding, String vocabularyId) {
    if (procedureCoding == null) {
      return false;
    }
    var codingVocabularyId = findOmopConcepts.getOmopVocabularyId(procedureCoding.getSystem());
    return codingVocabularyId.equals(vocabularyId);
  }
}
