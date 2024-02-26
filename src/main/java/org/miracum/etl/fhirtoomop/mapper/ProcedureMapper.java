package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_CONDITION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_DRUG;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_MEASUREMENT;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_PROCEDURE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_PROCEDURE_BODYSITE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_PROCEDURE_DICOM;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_IPRD;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_OPS;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_WHO;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

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
import org.miracum.etl.fhirtoomop.model.OpsStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.ConditionOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.DeviceExposure;
import org.miracum.etl.fhirtoomop.model.omop.DrugExposure;
import org.miracum.etl.fhirtoomop.model.omop.Measurement;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.model.omop.ProcedureOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.repository.service.DeviceExposureMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.ProcedureMapperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
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
      Arrays.asList(SOURCE_VOCABULARY_ID_PROCEDURE_DICOM, VOCABULARY_OPS, VOCABULARY_SNOMED, VOCABULARY_IPRD, VOCABULARY_WHO);

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

    String procedureId = "";
    if (!Strings.isNullOrEmpty(procedureLogicId)) {
      procedureId = srcProcedure.getId();
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingProcedureEntry(procedureLogicId, procedureSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted [Procedure] resource {}. Deleting from OMOP DB.", procedureId);
        return null;
      }
    }

    var statusElement = srcProcedure.getStatusElement();
    var statusValue = checkDataAbsentReason.getValue(statusElement);
    if (Strings.isNullOrEmpty(statusValue)
        || !FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status]: {} of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          statusValue,
          procedureId);
      return null;
    }

    var personId = getPersonId(srcProcedure, procedureLogicId, procedureId);
    if (personId == null) {
      log.warn("No matching [Person] found for [Procedure]: {}. Skip resource", procedureId);
      noPersonIdCounter.increment();
      return null;
    }

    var procedureCodings = getProcedureCodings(srcProcedure, procedureLogicId);
    if (procedureCodings.isEmpty()) {
      log.warn("No [Code] found in [Procedure]: {}. Skip resource", procedureId);
      noCodeCounter.increment();
      return null;
    }

    var procedureOnset = getProcedureOnset(srcProcedure);
    if (procedureOnset.getStartDateTime() == null) {
      log.warn(
          "Unable to determine [Performed DateTime] for [Procedure]: {}. Skip resource",
          procedureId);
      noStartDateCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcProcedure, personId, procedureId);

    createProcedureMapping(
        wrapper,
        procedureCodings,
        procedureOnset.getStartDateTime(),
        personId,
        visitOccId,
        procedureLogicId,
        procedureSourceIdentifier,
        srcProcedure,
        procedureId);

    var usedCodesCodeableConcepts = srcProcedure.getUsedCode();
    if (!usedCodesCodeableConcepts.isEmpty()) {
      var deviceExposures =
          createDeviceExposure(
              personId,
              visitOccId,
              procedureOnset,
              procedureLogicId,
              procedureSourceIdentifier,
              usedCodesCodeableConcepts,
              procedureId);
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
      List<CodeableConcept> usedCodesCodeableConcepts,
      String procedureId) {
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
              deviceCoding, startDateTime.toLocalDate(), bulkload, dbMappings, procedureId);
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
   */
  private void createProcedureMapping(
      OmopModelWrapper wrapper,
      List<Coding> procedureCodings,
      LocalDateTime procedureStartDatetime,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      Procedure srcProcedure,
      String procedureId) {

    var codingSize = procedureCodings.size();
    if (codingSize == 1) {
      setProcedureConceptsUsingSingleCoding(
          wrapper,
          procedureStartDatetime,
          procedureCodings.get(0),
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureId);
    } else {
      setProcedureConceptsUsingMultipleCodings(
          wrapper,
          procedureStartDatetime,
          procedureCodings,
          srcProcedure,
          personId,
          visitOccId,
          procedureLogicId,
          procedureSourceIdentifier,
          procedureId);
    }
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
      OmopModelWrapper wrapper,
      LocalDateTime procedureStartDatetime,
      Coding procedureCoding,
      Procedure srcProcedure,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      String procedureId) {

    List<Pair<String, List<OpsStandardDomainLookup>>> opsStandardMapPairList = null;
    SourceToConceptMap dicomConcept = null;
    Concept snomedConcept = null;
    Concept iprdConcept = null;
    Concept whoConcept = null;

    var procedureCodeExist =
        checkIfAnyProcedureCodesExist(procedureCoding, listOfProcedureVocabularyId);
    if (!procedureCodeExist) {
      return;
    }

    var procedureVocabularyId = findOmopConcepts.getOmopVocabularyId(procedureCoding.getSystem());

    var procedureBodySiteLocalization =
        getBodySiteLocalization(
            srcProcedure, procedureCoding, procedureStartDatetime.toLocalDate(), procedureId);

    if (procedureVocabularyId.equals(VOCABULARY_OPS)) {
      // for OPS codes

      opsStandardMapPairList =
          getValidOpsCodes(procedureCoding, procedureStartDatetime.toLocalDate(), procedureId);

      if (opsStandardMapPairList.isEmpty()) {
        return;
      }
      for (var singlePair : opsStandardMapPairList) {
        procedureProcessor(
            singlePair,
            null,
            null,
            wrapper,
            procedureBodySiteLocalization,
            procedureStartDatetime,
            procedureLogicId,
            procedureSourceIdentifier,
            personId,
            visitOccId,
            procedureId);
      }

    } else if (procedureVocabularyId.equals(SOURCE_VOCABULARY_ID_PROCEDURE_DICOM)) {
      // for DICOM codes

      dicomConcept =
          findOmopConcepts.getCustomConcepts(
              procedureCoding.getCode(), procedureVocabularyId, dbMappings);
      if (dicomConcept == null) {
        return;
      }

      procedureProcessor(
          null,
          null,
          dicomConcept,
          wrapper,
          procedureBodySiteLocalization,
          procedureStartDatetime,
          procedureLogicId,
          procedureSourceIdentifier,
          personId,
          visitOccId,
          procedureId);

    } else if (procedureVocabularyId.equals(VOCABULARY_SNOMED)) {
      // for SNOMED codes

      snomedConcept =
          findOmopConcepts.getConcepts(
              procedureCoding,
              procedureStartDatetime.toLocalDate(),
              bulkload,
              dbMappings,
              procedureId);

      if (snomedConcept == null) {
        return;
      }

      procedureProcessor(
          null,
          snomedConcept,
          null,
          wrapper,
          procedureBodySiteLocalization,
          procedureStartDatetime,
          procedureLogicId,
          procedureSourceIdentifier,
          personId,
          visitOccId,
          procedureId);
    }else if (procedureVocabularyId.equals(VOCABULARY_IPRD)) {
      // for IPRD codes

      iprdConcept =
              findOmopConcepts.getConcepts(
                      procedureCoding,
                      procedureStartDatetime.toLocalDate(),
                      bulkload,
                      dbMappings,
                      procedureId);

      if (iprdConcept == null) {
        return;
      }

      procedureProcessor(
              null,
              iprdConcept,
              null,
              wrapper,
              procedureBodySiteLocalization,
              procedureStartDatetime,
              procedureLogicId,
              procedureSourceIdentifier,
              personId,
              visitOccId,
              procedureId);
    }
    else if (procedureVocabularyId.equals(VOCABULARY_WHO)) {
      // for WHO codes

      whoConcept =
              findOmopConcepts.getConcepts(
                      procedureCoding,
                      procedureStartDatetime.toLocalDate(),
                      bulkload,
                      dbMappings,
                      procedureId);

      if (whoConcept == null) {
        return;
      }

      procedureProcessor(
              null,
              whoConcept,
              null,
              wrapper,
              procedureBodySiteLocalization,
              procedureStartDatetime,
              procedureLogicId,
              procedureSourceIdentifier,
              personId,
              visitOccId,
              procedureId);
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
      OmopModelWrapper wrapper,
      LocalDateTime procedureStartDatetime,
      List<Coding> procedureCodings,
      Procedure srcProcedure,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier,
      String procedureId) {

    Coding uncheckedOpsCoding = null;
    Coding uncheckedDicomCoding = null;
    Coding uncheckedSnomedCoding = null;
    Coding procedureCoding = null;

    for (var uncheckedCoding : procedureCodings) {
      var procedureVocabularyId = findOmopConcepts.getOmopVocabularyId(uncheckedCoding.getSystem());
      if (procedureVocabularyId.equals(VOCABULARY_OPS)) {
        uncheckedOpsCoding = uncheckedCoding;
      }
      if (procedureVocabularyId.equals(SOURCE_VOCABULARY_ID_PROCEDURE_DICOM)) {
        uncheckedDicomCoding = uncheckedCoding;
      }
      if (procedureVocabularyId.equals(VOCABULARY_SNOMED)) {
        uncheckedSnomedCoding = uncheckedCoding;
      }
    }
    if (uncheckedOpsCoding == null
        && uncheckedDicomCoding == null
        && uncheckedSnomedCoding == null) {
      return;
    }

    // OPS
    var opsStandardMapPairList =
        getValidOpsCodes(uncheckedOpsCoding, procedureStartDatetime.toLocalDate(), procedureId);

    // DICOM
    SourceToConceptMap dicomConcept = null;
    if (uncheckedDicomCoding != null) {
      dicomConcept =
          findOmopConcepts.getCustomConcepts(
              uncheckedDicomCoding.getCode(),
              findOmopConcepts.getOmopVocabularyId(uncheckedDicomCoding.getSystem()),
              dbMappings);
    }
    // SNOMED
    var snomedConcept =
        findOmopConcepts.getConcepts(
            uncheckedSnomedCoding,
            procedureStartDatetime.toLocalDate(),
            bulkload,
            dbMappings,
            procedureId);

    if (opsStandardMapPairList.isEmpty() && snomedConcept == null && dicomConcept == null) {
      return;
    } else if (!opsStandardMapPairList.isEmpty()) {
      // OPS
      procedureCoding = uncheckedOpsCoding;
    } else if (dicomConcept != null) {
      // DICOM
      procedureCoding = uncheckedDicomCoding;
    } else if (snomedConcept != null) {
      // SNOMED
      procedureCoding = uncheckedSnomedCoding;
    }

    setProcedureConceptsUsingSingleCoding(
        wrapper,
        procedureStartDatetime,
        procedureCoding,
        srcProcedure,
        personId,
        visitOccId,
        procedureLogicId,
        procedureSourceIdentifier,
        procedureId);
  }

  /**
   * Processes information from FHIR Procedure resource and transforms them into records OMOP CDM
   * tables.
   *
   * @param singlePair one pair of OPS code and its OMOP standard concept_id and domain information
   * @param omopConcept extracted Concept from OMOP
   * @param dicomConcept source_to_concept_map entry for DICOM code
   * @param wrapper the OMOP model wrapper
   * @param procedureDate date of the FHIR Procedure resource
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @param procedureSourceIdentifier identifier of the FHIR Procedure resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visiOccId visit_occurrence_id of the referenced FHIR Encounter resource
   */
  private void procedureProcessor(
      @Nullable Pair<String, List<OpsStandardDomainLookup>> opsStandardPair,
      @Nullable Concept snomedConcept,
      @Nullable SourceToConceptMap dicomConcept,
      OmopModelWrapper wrapper,
      Pair<String, Integer> procedureBodySiteLocalization,
      LocalDateTime procedureStartDatetime,
      String procedureLogicId,
      String procedureSourceIdentifier,
      Long personId,
      Long visitOccId,
      String procedureId) {

    if (opsStandardPair == null && snomedConcept == null && dicomConcept == null) {
      return;
    }

    if (opsStandardPair != null) {
      var opsCode = opsStandardPair.getLeft();
      var opsStandardMaps = opsStandardPair.getRight();

      for (var opsStandardMap : opsStandardMaps) {
        setProcedure(
            wrapper,
            procedureBodySiteLocalization,
            procedureStartDatetime,
            procedureLogicId,
            procedureSourceIdentifier,
            personId,
            visitOccId,
            opsCode,
            opsStandardMap.getStandardConceptId(),
            opsStandardMap.getSourceConceptId(),
            opsStandardMap.getStandardDomainId(),
            procedureId);
      }

    } else if (dicomConcept != null) {
      setProcedure(
          wrapper,
          procedureBodySiteLocalization,
          procedureStartDatetime,
          procedureLogicId,
          procedureSourceIdentifier,
          personId,
          visitOccId,
          dicomConcept.getSourceCode(),
          dicomConcept.getTargetConceptId(),
          dicomConcept.getTargetConceptId(),
          OMOP_DOMAIN_PROCEDURE,
          procedureId);

    } else {
      setProcedure(
          wrapper,
          procedureBodySiteLocalization,
          procedureStartDatetime,
          procedureLogicId,
          procedureSourceIdentifier,
          personId,
          visitOccId,
          snomedConcept.getConceptCode(),
          snomedConcept.getConceptId(),
          snomedConcept.getConceptId(),
          snomedConcept.getDomainId(),
          procedureId);
    }
  }

  /** Write procedure information into correct OMOP tables based on their domains. */
  private void setProcedure(
      OmopModelWrapper wrapper,
      Pair<String, Integer> procedureBodySiteLocalization,
      LocalDateTime procedureStartDatetime,
      String procedureLogicId,
      String procedureSourceIdentifier,
      Long personId,
      Long visitOccId,
      String procedureCode,
      Integer procedureConceptId,
      Integer procedureSourceConceptId,
      String domain,
      String procedureId) {
    if(domain == null){
      log.warn("fhirId = {}={}",procedureId,domain);
      return;
    }
    switch (domain) {
      case OMOP_DOMAIN_PROCEDURE:
        var procedure =
            setUpProcedure(
                procedureStartDatetime,
                procedureConceptId,
                procedureSourceConceptId,
                procedureCode,
                personId,
                visitOccId,
                procedureBodySiteLocalization,
                procedureLogicId,
                procedureSourceIdentifier);

        wrapper.getProcedureOccurrence().add(procedure);

        break;
      case OMOP_DOMAIN_OBSERVATION:
        var observation =
            setUpObservation(
                procedureStartDatetime,
                procedureConceptId,
                procedureSourceConceptId,
                procedureCode,
                personId,
                visitOccId,
                procedureBodySiteLocalization,
                procedureLogicId,
                procedureSourceIdentifier);

        wrapper.getObservation().add(observation);

        break;
      case OMOP_DOMAIN_DRUG:
        var drug =
            setUpDrug(
                procedureStartDatetime,
                procedureConceptId,
                procedureSourceConceptId,
                procedureCode,
                personId,
                visitOccId,
                procedureLogicId,
                procedureSourceIdentifier);

        wrapper.getDrugExposure().add(drug);

        break;
      case OMOP_DOMAIN_MEASUREMENT:
        var measurement =
            setUpMeasurement(
                procedureStartDatetime,
                procedureConceptId,
                procedureSourceConceptId,
                procedureCode,
                personId,
                visitOccId,
                procedureBodySiteLocalization,
                procedureLogicId,
                procedureSourceIdentifier);

        wrapper.getMeasurement().add(measurement);

        break;
      case OMOP_DOMAIN_CONDITION:
        var condition =
                setUpCondition(
                        procedureStartDatetime,
                        procedureConceptId,
                        procedureSourceConceptId,
                        procedureCode,
                        personId,
                        visitOccId,
                        procedureLogicId,
                        procedureSourceIdentifier);

        wrapper.getConditionOccurrence().add(condition);

        break;

      default:
        //        throw new UnsupportedOperationException(String.format("Unsupported domain %s",
        // domain));
        log.error(
            "[Unsupported domain] {} of code in [Procedure]: {}. Skip resource.",
            domain,
            procedureId);
        break;
    }
  }

  private ConditionOccurrence setUpCondition(
          LocalDateTime procedureStartDatetime,
          Integer diagnoseConceptId,
          Integer diagnoseSourceConceptId,
          String rawIcdCode,
          Long personId,
          Long visitOccId,
          String conditionLogicId,
          String conditionSourceIdentifier) {

    return ConditionOccurrence.builder()
            .personId(personId)
            .conditionStartDate(procedureStartDatetime.toLocalDate())
            .conditionStartDatetime(procedureStartDatetime)
            .visitOccurrenceId(visitOccId)
            .conditionSourceConceptId(diagnoseSourceConceptId)
            .conditionConceptId(diagnoseConceptId)
            .conditionTypeConceptId(CONCEPT_EHR)
            .conditionSourceValue(rawIcdCode)
            .fhirLogicalId(conditionLogicId)
            .fhirIdentifier(conditionSourceIdentifier)
            .build();
  }

  private ProcedureOccurrence setUpProcedure(
      LocalDateTime procedureStartDatetime,
      Integer procedureConceptId,
      Integer procedureSourceConceptId,
      String procedureCode,
      Long personId,
      Long visitOccId,
      Pair<String, Integer> procedureBodySiteLocalization,
      String procedureLogicId,
      String procedureSourceIdentifier) {

    var newProcedureOccurrence =
        ProcedureOccurrence.builder()
            .personId(personId)
            .procedureDate(procedureStartDatetime.toLocalDate())
            .procedureDatetime(procedureStartDatetime)
            .visitOccurrenceId(visitOccId)
            .procedureSourceConceptId(procedureSourceConceptId)
            .procedureConceptId(procedureConceptId)
            .procedureTypeConceptId(CONCEPT_EHR)
            .procedureSourceValue(procedureCode)
            .fhirLogicalId(procedureLogicId)
            .fhirIdentifier(procedureSourceIdentifier)
            .build();

    if (procedureBodySiteLocalization != null) {
      newProcedureOccurrence.setModifierSourceValue(procedureBodySiteLocalization.getLeft());
      newProcedureOccurrence.setModifierConceptId(procedureBodySiteLocalization.getRight());
    }

    return newProcedureOccurrence;
  }

  private OmopObservation setUpObservation(
      LocalDateTime procedureStartDatetime,
      Integer procedureConceptId,
      Integer procedureSourceConceptId,
      String procedureCode,
      Long personId,
      Long visitOccId,
      Pair<String, Integer> procedureBodySiteLocalization,
      String procedureLogicId,
      String procedureSourceIdentifier) {

    var newObservation =
        OmopObservation.builder()
            .personId(personId)
            .observationDate(procedureStartDatetime.toLocalDate())
            .observationDatetime(procedureStartDatetime)
            .visitOccurrenceId(visitOccId)
            .observationSourceConceptId(procedureSourceConceptId)
            .observationConceptId(procedureConceptId)
            .observationTypeConceptId(CONCEPT_EHR)
            .observationSourceValue(procedureCode)
            .fhirLogicalId(procedureLogicId)
            .fhirIdentifier(procedureSourceIdentifier)
            .build();

    if (procedureBodySiteLocalization != null) {
      newObservation.setQualifierSourceValue(procedureBodySiteLocalization.getLeft());
      newObservation.setQualifierConceptId(procedureBodySiteLocalization.getRight());
    }

    return newObservation;
  }

  private Measurement setUpMeasurement(
      LocalDateTime procedureStartDatetime,
      Integer procedureConceptId,
      Integer procedureSourceConceptId,
      String procedureCode,
      Long personId,
      Long visitOccId,
      Pair<String, Integer> procedureBodySiteLocalization,
      String procedureLogicId,
      String procedureSourceIdentifier) {

    var newMeasurement =
        Measurement.builder()
            .personId(personId)
            .measurementDate(procedureStartDatetime.toLocalDate())
            .measurementDatetime(procedureStartDatetime)
            .visitOccurrenceId(visitOccId)
            .measurementSourceConceptId(procedureSourceConceptId)
            .measurementConceptId(procedureConceptId)
            .measurementTypeConceptId(CONCEPT_EHR)
            .measurementSourceValue(procedureCode)
            .fhirLogicalId(procedureLogicId)
            .fhirIdentifier(procedureSourceIdentifier)
            .build();

    if (procedureBodySiteLocalization != null) {
      newMeasurement.setValueSourceValue(procedureBodySiteLocalization.getLeft());
      newMeasurement.setValueAsConceptId(procedureBodySiteLocalization.getRight());
    }

    return newMeasurement;
  }

  private DrugExposure setUpDrug(
      LocalDateTime procedureStartDatetime,
      Integer procedureConceptId,
      Integer procedureSourceConceptId,
      String procedureCode,
      Long personId,
      Long visitOccId,
      String procedureLogicId,
      String procedureSourceIdentifier) {

    return DrugExposure.builder()
        .personId(personId)
        .drugExposureStartDate(procedureStartDatetime.toLocalDate())
        .drugExposureStartDatetime(procedureStartDatetime)
        .drugExposureEndDate(procedureStartDatetime.toLocalDate())
        .visitOccurrenceId(visitOccId)
        .drugSourceConceptId(procedureSourceConceptId)
        .drugConceptId(procedureConceptId)
        .drugTypeConceptId(CONCEPT_EHR)
        .drugSourceValue(procedureCode)
        .fhirLogicalId(procedureLogicId)
        .fhirIdentifier(procedureSourceIdentifier)
        .build();
  }

  /**
   * Extract valid pairs of OPS code and its OMOP concept_id and domain information as a list
   *
   * @param opsCoding
   * @param procedureDate the date of procedure
   * @return a list of valid pairs of OPS code and its OMOP concept_id and domain information
   */
  private List<Pair<String, List<OpsStandardDomainLookup>>> getValidOpsCodes(
      Coding opsCoding, LocalDate procedureDate, String procedureId) {
    if (opsCoding == null) {
      return Collections.emptyList();
    }

    List<Pair<String, List<OpsStandardDomainLookup>>> validOpsStandardConceptMaps =
        new ArrayList<>();
    List<OpsStandardDomainLookup> opsStandardMap =
        findOmopConcepts.getOpsStandardConcepts(
            opsCoding, procedureDate, bulkload, dbMappings, procedureId);
    if (opsStandardMap.isEmpty()) {
      return Collections.emptyList();
    }

    validOpsStandardConceptMaps.add(Pair.of(opsCoding.getCode(), opsStandardMap));

    return validOpsStandardConceptMaps;
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
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR Procedure
   * resource.
   *
   * @param srcProcedure FHIR Procedure resource
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Procedure srcProcedure, String procedureLogicId, String procedureId) {
    var patientReferenceIdentifier = fhirReferenceUtils.getSubjectReferenceIdentifier(srcProcedure);
    var patientReferenceLogicalId = fhirReferenceUtils.getSubjectReferenceLogicalId(srcProcedure);
    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, procedureLogicId, procedureId);
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
  private Long getVisitOccId(Procedure srcProcedure, Long personId, String procedureId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcProcedure);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcProcedure);

    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier, encounterReferenceLogicalId, personId, procedureId);

    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for [Procedure]: {}.", procedureId);
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
      Procedure srcProcedure, Coding procedureCoding, LocalDate procedureDate, String procedureId) {
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
        findOmopConcepts.getConcepts(
            procedureBodySite, procedureDate, bulkload, dbMappings, procedureId);
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
    return vocabularyId.contains(codingVocabularyId);
  }

  private boolean checkIfSpecificProcedureCodesExist(Coding procedureCoding, String vocabularyId) {
    if (procedureCoding == null) {
      return false;
    }
    var codingVocabularyId = findOmopConcepts.getOmopVocabularyId(procedureCoding.getSystem());
    return codingVocabularyId.equals(vocabularyId);
  }

  /**
   * Deletes FHIR Procedure resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param procedureLogicId logical id of the FHIR Procedure resource
   * @param procedureSourceIdentifier identifier of the FHIR Procedure resource
   */
  private void deleteExistingProcedureEntry(
      String procedureLogicId, String procedureSourceIdentifier) {
    if (!Strings.isNullOrEmpty(procedureLogicId)) {
      procedureService.deleteExistingProceduresByFhirLogicalId(procedureLogicId);
    } else {
      procedureService.deleteExistingProceduresByFhirIdentifier(procedureSourceIdentifier);
    }
  }
}
