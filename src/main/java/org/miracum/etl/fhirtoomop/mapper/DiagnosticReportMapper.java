package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_DIAGNOSTIC_REPORT_ACCEPTABLE_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_MEASUREMENT;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_PROCEDURE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_DIAGNOSTIC_REPORT_CATEGORY;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DiagnosticReport;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.Measurement;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.model.omop.ProcedureOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.repository.service.DiagnosticReportMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DiagnosticReportMapper implements FhirMapper<DiagnosticReport> {
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final DbMappings dbMappings;
  private final Boolean bulkload;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;
  @Autowired DiagnosticReportMapperServiceImpl diagnosticReportService;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessDiagnosticReport");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessDiagnosticReport");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessDiagnosticReport");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessDiagnosticReport");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessDiagnosticReport");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessDiagnosticReport");

  @Autowired
  public DiagnosticReportMapper(DbMappings dbMappings, Boolean bulkload) {
    this.dbMappings = dbMappings;
    this.bulkload = bulkload;
  }

  @Override
  public OmopModelWrapper map(DiagnosticReport srcDiagnosticReport, boolean isDeleted) {
    var wrapper = new OmopModelWrapper();

    var diagnosticReportLogicId = fhirReferenceUtils.extractId(srcDiagnosticReport);

    var diagnosticReportSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcDiagnosticReport);
    if (Strings.isNullOrEmpty(diagnosticReportLogicId)
        && Strings.isNullOrEmpty(diagnosticReportSourceIdentifier)) {
      log.warn(
          "No [Identifier] or [Id] found. [DiagnosticReport] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }
    String diagnosticReportId = "";
    if (!Strings.isNullOrEmpty(diagnosticReportLogicId)) {
      diagnosticReportId = srcDiagnosticReport.getId();
    }

    if (Boolean.FALSE.equals(bulkload)) {
      deleteExistingDiagnosticReport(diagnosticReportLogicId, diagnosticReportSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", diagnosticReportLogicId);
        return null;
      }
    }

    var status = getStatus(srcDiagnosticReport);
    if (status == null) {
      log.error(
          "[status] {} from {} is not acceptible. Skip resource.", status, diagnosticReportLogicId);
      return null;
    }

    var personId = getPersonId(srcDiagnosticReport, diagnosticReportLogicId, diagnosticReportId);
    if (personId == null) {
      log.warn(
          "No matching [Person] found for [DiagnosticReport]: {}. Skip resource",
          diagnosticReportLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var diagnosticReportOnset = getDiagnosticReportOnset(srcDiagnosticReport);
    if (diagnosticReportOnset.getStartDateTime() == null) {
      log.warn(
          "No [Date] found for [DiagnosticReport]: {}. Skip resource", diagnosticReportLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var diagnosticReportCategoryLoincCoding = getCategoryLoincCoding(srcDiagnosticReport);
    if (diagnosticReportCategoryLoincCoding == null) {
      log.warn(
          "No [Category Loinc Code] found for [DiagnosticReport]: {}. Skip resource",
          diagnosticReportLogicId);
      return null;
    }

    var diagnosticReportLoincCoding = getLoincCoding(srcDiagnosticReport);
    if (diagnosticReportLoincCoding == null) {
      log.warn(
          "No [Loinc code] found for [DiagnosticReport]: {}. Skip resource",
          diagnosticReportLogicId);
      noCodeCounter.increment();
      return null;
    }

    var conclusionCoding = getConclusionCoding(srcDiagnosticReport);
    if (conclusionCoding == null) {
      log.warn(
          "No [Conclusion Code] found for [DiagnosticReport]: {}. Skip resource",
          diagnosticReportLogicId);
      return null;
    }

    var visitOccId = getVisitOccId(srcDiagnosticReport, diagnosticReportId, personId);

    setDiagnosticReport(
        wrapper,
        personId,
        visitOccId,
        diagnosticReportOnset,
        diagnosticReportCategoryLoincCoding,
        diagnosticReportLoincCoding,
        conclusionCoding,
        diagnosticReportLogicId,
        diagnosticReportSourceIdentifier,
        diagnosticReportId);

    return wrapper;
  }

  private void setDiagnosticReport(
      OmopModelWrapper wrapper,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnosticReportOnset,
      Coding diagnosticReportCategoryLoincCoding,
      Coding diagnosticReportLoincCoding,
      Coding conclusionCoding,
      String diagnosticReportLogicId,
      String diagnosticReportSourceIdentifier) {
    var loincCodingConcept =
        findOmopConcepts.getConcepts(
            diagnosticReportLoincCoding,
            diagnosticReportOnset.getStartDateTime().toLocalDate(),
            bulkload,
            dbMappings);
    if (loincCodingConcept == null) {
      return;
    }
    var categoryCodingConcept =
        findOmopConcepts.getCustomConcepts(
            diagnosticReportCategoryLoincCoding,
            SOURCE_VOCABULARY_ID_DIAGNOSTIC_REPORT_CATEGORY,
            dbMappings);
    var domainId = loincCodingConcept.getDomainId();
    switch (domainId) {
      case OMOP_DOMAIN_OBSERVATION:
        createDiagnosticReportObservation(
            wrapper,
            personId,
            visitOccId,
            diagnosticReportOnset,
            categoryCodingConcept,
            loincCodingConcept,
            conclusionCoding,
            diagnosticReportLogicId,
            diagnosticReportSourceIdentifier);
        break;
      case OMOP_DOMAIN_MEASUREMENT:
        createDiagnosticReportMeasurement(
            wrapper,
            personId,
            visitOccId,
            diagnosticReportOnset,
            categoryCodingConcept,
            loincCodingConcept,
            conclusionCoding,
            diagnosticReportLogicId,
            diagnosticReportSourceIdentifier);
        break;
      case OMOP_DOMAIN_PROCEDURE:
        createDiagnosticReportProcedureOcc(
            wrapper,
            personId,
            visitOccId,
            diagnosticReportOnset,
            categoryCodingConcept,
            loincCodingConcept,
            conclusionCoding,
            diagnosticReportLogicId,
            diagnosticReportSourceIdentifier);
        break;
      default:
        log.warn("");
        break;
    }
  }

  private void createDiagnosticReportProcedureOcc(
      OmopModelWrapper wrapper,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnosticReportOnset,
      SourceToConceptMap categoryCodingConcept,
      Concept loincCodingConcept,
      Coding conclusionCoding,
      String diagnosticReportLogicId,
      String diagnosticReportSourceIdentifier) {

    var conclusionSnomedCodingList = getSnomedCodingList(conclusionCoding);
    if (conclusionSnomedCodingList.isEmpty()) {
      return;
    }
    for (var snomedCoding : conclusionSnomedCodingList) {
      var modifiedSnomedCoding = getModifiedConclusionCode(snomedCoding);
      var snomedConcept =
          findOmopConcepts.getConcepts(
              modifiedSnomedCoding,
              diagnosticReportOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);

      if (snomedConcept == null) {
        continue;
      }

      var diagnosticReportProcedure =
          ProcedureOccurrence.builder()
              .personId(personId)
              .visitOccurrenceId(visitOccId)
              .procedureTypeConceptId(categoryCodingConcept.getTargetConceptId())
              .procedureConceptId(loincCodingConcept.getConceptId())
              //              .procedureSourceConceptId(loincCodingConcept.getConceptId())
              .procedureSourceConceptId(snomedConcept.getConceptId())
              .procedureSourceValue(snomedCoding.getCode())
              //              .procedureSourceValue(loincCodingConcept.getConceptCode())
              //              .modifierConceptId(snomedConcept.getConceptId())
              //              .modifierSourceValue(snomedCoding.getCode())
              .procedureDate(diagnosticReportOnset.getStartDateTime().toLocalDate())
              .procedureDatetime(diagnosticReportOnset.getStartDateTime())
              .fhirIdentifier(diagnosticReportSourceIdentifier)
              .fhirLogicalId(diagnosticReportLogicId)
              .build();
      var interpretationCoding = getConclusionCodingInterpretationCoding(snomedCoding);
      var interpretationConcept =
          findOmopConcepts.getConcepts(
              interpretationCoding,
              diagnosticReportOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);
      if (interpretationConcept != null) {
        diagnosticReportProcedure.setModifierConceptId(interpretationConcept.getConceptId());
        diagnosticReportProcedure.setModifierSourceValue(interpretationConcept.getConceptCode());
      }

      wrapper.getProcedureOccurrence().add(diagnosticReportProcedure);
    }
  }

  private void createDiagnosticReportMeasurement(
      OmopModelWrapper wrapper,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnosticReportOnset,
      SourceToConceptMap categoryCodingConcept,
      Concept loincCodingConcept,
      Coding conclusionCoding,
      String diagnosticReportLogicId,
      String diagnosticReportSourceIdentifier) {

    var conclusionSnomedCodingList = getSnomedCodingList(conclusionCoding);
    if (conclusionSnomedCodingList.isEmpty()) {
      return;
    }
    for (var snomedCoding : conclusionSnomedCodingList) {
      var modifiedSnomedCoding = getModifiedConclusionCode(snomedCoding);
      var snomedConcept =
          findOmopConcepts.getConcepts(
              modifiedSnomedCoding,
              diagnosticReportOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);

      if (snomedConcept == null) {
        continue;
      }

      var diagnosticReportMeasurement =
          Measurement.builder()
              .personId(personId)
              .visitOccurrenceId(visitOccId)
              .measurementTypeConceptId(categoryCodingConcept.getTargetConceptId())
              .measurementConceptId(loincCodingConcept.getConceptId())
              .measurementSourceConceptId(snomedConcept.getConceptId())
              .measurementSourceValue(snomedCoding.getCode())
              //              .measurementSourceConceptId(loincCodingConcept.getConceptId())
              //              .measurementSourceValue(loincCodingConcept.getConceptCode())
              //              .valueAsConceptId(snomedConcept.getConceptId())
              .valueSourceValue(snomedCoding.getCode())
              .measurementDate(diagnosticReportOnset.getStartDateTime().toLocalDate())
              .measurementDatetime(diagnosticReportOnset.getStartDateTime())
              .fhirIdentifier(diagnosticReportSourceIdentifier)
              .fhirLogicalId(diagnosticReportLogicId)
              .build();

      var interpretationCoding = getConclusionCodingInterpretationCoding(snomedCoding);
      var interpretationConcept =
          findOmopConcepts.getConcepts(
              interpretationCoding,
              diagnosticReportOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);

      if (interpretationConcept != null) {
        diagnosticReportMeasurement.setOperatorConceptId(interpretationConcept.getConceptId());
      }
      wrapper.getMeasurement().add(diagnosticReportMeasurement);
    }
  }

  private void createDiagnosticReportObservation(
      OmopModelWrapper wrapper,
      Long personId,
      Long visitOccId,
      ResourceOnset diagnosticReportOnset,
      SourceToConceptMap categoryCodingConcept,
      Concept loincCodingConcept,
      Coding conclusionCoding,
      String diagnosticReportLogicId,
      String diagnosticReportSourceIdentifier) {

    var conclusionSnomedCodingList = getSnomedCodingList(conclusionCoding);
    if (conclusionSnomedCodingList.isEmpty()) {
      return;
    }
    for (var snomedCoding : conclusionSnomedCodingList) {
      var modifiedSnomedCoding = getModifiedConclusionCode(snomedCoding);

      var snomedConcept =
          findOmopConcepts.getConcepts(
              modifiedSnomedCoding,
              diagnosticReportOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);

      if (snomedConcept == null) {
        continue;
      }

      var diagnosticReportObservation =
          OmopObservation.builder()
              .personId(personId)
              .visitOccurrenceId(visitOccId)
              .observationTypeConceptId(categoryCodingConcept.getTargetConceptId())
              .observationConceptId(loincCodingConcept.getConceptId())
              .observationSourceConceptId(snomedConcept.getConceptId())
              .observationSourceValue(snomedCoding.getCode())
              //              .observationSourceConceptId(loincCodingConcept.getConceptId())
              //              .observationSourceValue(loincCodingConcept.getConceptCode())
              //              .valueAsConceptId(snomedConcept.getConceptId())
              .valueAsString(snomedCoding.getCode())
              .observationDate(diagnosticReportOnset.getStartDateTime().toLocalDate())
              .observationDatetime(diagnosticReportOnset.getStartDateTime())
              .fhirIdentifier(diagnosticReportSourceIdentifier)
              .fhirLogicalId(diagnosticReportLogicId)
              .build();

      var interpretationCoding = getConclusionCodingInterpretationCoding(snomedCoding);
      var interpretationConcept =
          findOmopConcepts.getConcepts(
              interpretationCoding,
              diagnosticReportOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);
      if (interpretationConcept != null) {
        diagnosticReportObservation.setQualifierConceptId(interpretationConcept.getConceptId());
        diagnosticReportObservation.setQualifierSourceValue(interpretationConcept.getConceptCode());
      }

      wrapper.getObservation().add(diagnosticReportObservation);
    }
  }

  private Coding getConclusionCodingInterpretationCoding(Coding conclusionSnomedCoding) {
    var snomedCode = conclusionSnomedCoding.getCode();
    if (!snomedCode.contains(":{")) {
      return null;
    }
    var colonPosition = snomedCode.indexOf(":");
    var conclusionAttributes = snomedCode.substring(colonPosition + 2, snomedCode.length() - 1);
    var conclusionInterpretationKeyValuePair = conclusionAttributes.split(",")[0];
    var conclusionInterpretationCode =
        conclusionInterpretationKeyValuePair.substring(
            conclusionInterpretationKeyValuePair.indexOf("=") + 1);

    return new Coding()
        .setCode(conclusionInterpretationCode)
        .setSystem(fhirSystems.getSnomed())
        .setVersion(conclusionSnomedCoding.getVersion());

    //    var splitedSnomedCode = Arrays.asList(snomedCode.split(":"));
    //    if (!splitedSnomedCode.get(0).equals("118247008")) {
    //      return null;
    //    }
    //    // from {363713009=373068000} to 363713009=373068000
    //    var extraInformationPart =
    //        splitedSnomedCode.get(1).substring(1, splitedSnomedCode.get(1).length() - 1);
    //    var splitedExtraInformation = Arrays.asList(extraInformationPart.split(","));
    //    for (var info : splitedExtraInformation) {
    //      if (info.contains("363713009")) {
    //        var splitedInterpretation = Arrays.asList(info.split("="));
    //        var interpretation = splitedInterpretation.get(1);
    //        return new Coding().setCode(interpretation).setSystem(fhirSystems.getSnomed());
    //      }
    //    }
    //    return null;
  }

  private Coding getConclusionCoding(DiagnosticReport srcDiagnosticReport) {
    if (!srcDiagnosticReport.hasConclusionCode()) {
      return null;
    }
    var conclusionCodeableConceptList = srcDiagnosticReport.getConclusionCode();
    if (conclusionCodeableConceptList.isEmpty()) {

      return null;
    }
    for (var codeableConcept : conclusionCodeableConceptList) {
      if (!codeableConcept.hasCoding()) {

        continue;
      }
      var codingList = codeableConcept.getCoding();
      if (codingList.isEmpty()) {

        continue;
      }
      var snomedCoding =
          codingList.stream()
              .filter(coding -> coding.getSystem().equals(fhirSystems.getSnomed()))
              .findFirst();
      if (snomedCoding.isPresent()) {
        return snomedCoding.get();
      }
    }

    return null;
  }

  private Coding getLoincCoding(DiagnosticReport srcDiagnosticReport) {
    if (!srcDiagnosticReport.hasCode()) {
      return null;
    }
    var codeableConcept = checkDataAbsentReason.getValue(srcDiagnosticReport.getCode());
    if (codeableConcept == null) {
      return null;
    }
    var loincCoding =
        codeableConcept.getCoding().stream()
            .filter(coding -> coding.getSystem().equals(fhirSystems.getLoinc()))
            .findFirst();
    if (loincCoding.isPresent()) {
      return loincCoding.get();
    }

    return null;
  }

  private Coding getCategoryLoincCoding(DiagnosticReport srcDiagnosticReport) {
    if (!srcDiagnosticReport.hasCategory()) {
      return null;
    }
    var categoryCodeableConceptList = srcDiagnosticReport.getCategory();
    if (categoryCodeableConceptList.isEmpty()) {
      return null;
    }
    for (var categoryCodeableConcept : categoryCodeableConceptList) {
      if (!categoryCodeableConcept.hasCoding()) {
        continue;
      }
      var categoryCodingList = categoryCodeableConcept.getCoding();
      if (categoryCodingList.isEmpty()) {
        continue;
      }
      var categoryLoincCoding =
          categoryCodingList.stream()
              .filter(
                  coding -> fhirSystems.getDiagnosticReportCategory().contains(coding.getSystem()))
              .findFirst();
      if (categoryLoincCoding.isPresent()) {
        return categoryLoincCoding.get();
      }
    }
    return null;
  }

  private void deleteExistingDiagnosticReport(
      String diagnosticReportLogicId, String diagnosticReportSourceIdentifier) {
    if (!Strings.isNullOrEmpty(diagnosticReportLogicId)) {
      diagnosticReportService.deleteExistingDiagnosticReportByFhirLogicalId(
          diagnosticReportLogicId);
    } else {
      diagnosticReportService.deleteExistingDiagnosticReportByFhirIdentifier(
          diagnosticReportSourceIdentifier);
    }
  }

  private List<Coding> getSnomedCodingList(Coding snomedCoding) {
    var snomedCodes = snomedCoding.getCode();
    if (Strings.isNullOrEmpty(snomedCodes)) {
      return Collections.emptyList();
    }

    if (snomedCodes.contains("+")) {
      var snomedCodesList = Arrays.asList(snomedCodes.split("\\+"));

      List<Coding> snomedCodingList = new ArrayList<>();

      for (var snomedCode : snomedCodesList) {

        var newSnomedCoding = new Coding();
        newSnomedCoding.setVersion(snomedCoding.getVersion());
        newSnomedCoding.setSystem(snomedCoding.getSystem());
        newSnomedCoding.setCode(snomedCode);
        snomedCodingList.add(newSnomedCoding);
      }
      return snomedCodingList;
    } else {
      return Arrays.asList(snomedCoding);
    }
  }

  private Coding getModifiedConclusionCode(Coding snomedCoding) {
    var snomedCodes = snomedCoding.getCode();
    if (snomedCodes.contains(":{")) {
      var colonPosition = snomedCodes.indexOf(":");
      var conclusionCode = snomedCodes.substring(0, colonPosition);

      return new Coding()
          .setSystem(snomedCoding.getSystem())
          .setVersion(snomedCoding.getVersion())
          .setCode(conclusionCode);
    }
    return snomedCoding;
  }

  private String getStatus(DiagnosticReport srcDiagnosticReport) {
    var statusElement = srcDiagnosticReport.getStatusElement();
    if (!statusElement.isEmpty()) {
      var status = checkDataAbsentReason.getValue(statusElement);
      if (FHIR_RESOURCE_DIAGNOSTIC_REPORT_ACCEPTABLE_STATUS_LIST.contains(status)) {
        return status;
      }
    }
    return null;
  }

  private ResourceOnset getDiagnosticReportOnset(DiagnosticReport srcDiagnosticReport) {
    var resourceOnset = new ResourceOnset();

    if (srcDiagnosticReport.hasEffectiveDateTimeType()) {
      resourceOnset.setStartDateTime(
          checkDataAbsentReason.getValue(srcDiagnosticReport.getEffectiveDateTimeType()));
      return resourceOnset;
    }

    if (srcDiagnosticReport.hasEffectivePeriod()) {
      var effectivePeriod =
          checkDataAbsentReason.getValue(srcDiagnosticReport.getEffectivePeriod());
      if (effectivePeriod == null) {
        return resourceOnset;
      }
      if (effectivePeriod.hasStartElement()) {
        resourceOnset.setStartDateTime(
            checkDataAbsentReason.getValue(effectivePeriod.getStartElement()));
      }
      if (effectivePeriod.hasEndElement()) {
        resourceOnset.setEndDateTime(
            checkDataAbsentReason.getValue(effectivePeriod.getEndElement()));
      }
      return resourceOnset;
    }

    return resourceOnset;
  }

  private Long getVisitOccId(
      DiagnosticReport srcDiagnosticReport, String diagnosticReportId, Long personId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcDiagnosticReport);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcDiagnosticReport);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            diagnosticReportId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for [DiagnosticReport]: {}.", diagnosticReportId);
    }

    return visitOccId;
  }

  private Long getPersonId(
      DiagnosticReport srcDiagnosticReport,
      String diagnosticReportLogicId,
      String diagnosticReportId) {
    var patientReferenceIdentifier =
        fhirReferenceUtils.getSubjectReferenceIdentifier(srcDiagnosticReport);
    var patientReferenceLogicalId =
        fhirReferenceUtils.getSubjectReferenceLogicalId(srcDiagnosticReport);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier,
        patientReferenceLogicalId,
        diagnosticReportLogicId,
        diagnosticReportId);
  }
}
