package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Immunization;
import org.hl7.fhir.r4.model.Quantity;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.DrugExposure;
import org.miracum.etl.fhirtoomop.repository.service.DrugExposureMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ImmunizationMapper implements FhirMapper<Immunization> {
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final DbMappings dbMappings;
  private final Boolean bulkload;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;
  @Autowired DrugExposureMapperServiceImpl drugExposureService;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessImmunizations");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessImmunizations");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessImmunizations");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessImmunizations");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessImmunizations");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessImmunizations");

  @Autowired
  public ImmunizationMapper(DbMappings dbMappings, Boolean bulkload) {
    this.dbMappings = dbMappings;
    this.bulkload = bulkload;
  }

  @Override
  public OmopModelWrapper map(Immunization srcImmunization, boolean isDeleted) {
    var wrapper = new OmopModelWrapper();

    var immunizationLogicId = fhirReferenceUtils.extractId(srcImmunization);

    var immunizationSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcImmunization);
    if (Strings.isNullOrEmpty(immunizationLogicId)
        && Strings.isNullOrEmpty(immunizationSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Immunization] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }

    if (Boolean.FALSE.equals(bulkload)) {
      deleteExistingImmunization(immunizationLogicId, immunizationSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", immunizationLogicId);
        return null;
      }
    }

    var status = getStatus(srcImmunization);
    if (status == null || !FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST.contains(status)) {
      log.error(
          "[status] {} from {} is not acceptable. Skip resource.", status, immunizationLogicId);
      return null;
    }

    var personId = getPersonId(srcImmunization, immunizationLogicId);
    if (personId == null) {
      log.warn(
          "No matching [Person] found for [Immunization]: {}. Skip resource", immunizationLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var immunizationOnset = getImmunizationOnset(srcImmunization);
    if (immunizationOnset.getStartDateTime() == null) {
      log.warn("No [Date] found for [Immunization]: {}. Skip resource", immunizationLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var vaccineCodingList = getVaccineCoding(srcImmunization);
    if (vaccineCodingList.isEmpty()
        || (vaccineCodingList.size() == 1
            && vaccineCodingList
                .get(0)
                .getSystem()
                .equals(fhirSystems.getVaccineStatusUnknown()))) {
      log.error("No [vaccine code] found for {}. Skip resource.", immunizationLogicId);
      noCodeCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcImmunization, immunizationLogicId, personId);
    var route = getRoute(srcImmunization);
    var dose = getDose(srcImmunization);

    var newDrugExposure =
        createDrugExposure(
            dose,
            route,
            vaccineCodingList,
            personId,
            visitOccId,
            immunizationOnset,
            immunizationLogicId,
            immunizationSourceIdentifier);

    wrapper.setDrugExposure(newDrugExposure);

    return wrapper;
  }

  private Quantity getDose(Immunization srcImmunization) {
    return srcImmunization.getDoseQuantity();
  }

  private void deleteExistingImmunization(
      String immunizationLogicId, String immunizationSourceIdentifier) {
    if (!Strings.isNullOrEmpty(immunizationLogicId)) {
      drugExposureService.deleteExistingDrugExposureByFhirLogicalId(immunizationLogicId);
    } else {
      drugExposureService.deleteExistingDrugExposureByFhirIdentifier(immunizationSourceIdentifier);
    }
  }

  private DrugExposure setBasisDrugExposure(
      Quantity dose,
      String route,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier) {

    var startDateTime = immunizationOnset.getStartDateTime();
    var endDateTime = immunizationOnset.getEndDateTime();
    String doseUnit = null;
    BigDecimal doseQuantity = null;

    if (dose != null) {
      doseUnit = dose.getUnit();
      doseQuantity = dose.getValue();
    }

    return DrugExposure.builder()
        .personId(personId)
        .visitOccurrenceId(visitOccId)
        .drugExposureStartDate(startDateTime.toLocalDate())
        .drugExposureStartDatetime(startDateTime)
        .drugExposureEndDate(endDateTime.toLocalDate())
        .routeSourceValue(route)
        .doseUnitSourceValue(doseUnit)
        .quantity(doseQuantity)
        .fhirLogicalId(immunizationLogicId)
        .fhirIdentifier(immunizationSourceIdentifier)
        .drugTypeConceptId(CONCEPT_EHR)
        .build();
  }

  private List<DrugExposure> createDrugExposure(
      Quantity dose,
      String route,
      List<Coding> vaccineCodingList,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier) {
    List<DrugExposure> vaccineDrugExposure = new ArrayList<>();
    var snomedCoding = getCoding(vaccineCodingList, Arrays.asList(fhirSystems.getSnomed()));
    var atcCoding = getCoding(vaccineCodingList, fhirSystems.getAtc());
    var snomedCodeExists = checkIfCodeExists(snomedCoding);
    var atcCodeExists = checkIfCodeExists(atcCoding);

    if (snomedCodeExists) {
      vaccineDrugExposure =
          setSnomedVaccineCode(
              dose,
              route,
              personId,
              visitOccId,
              immunizationOnset,
              immunizationLogicId,
              immunizationSourceIdentifier,
              snomedCoding);
    }
    if (atcCodeExists) {
      var atcDrugExposure =
          setAtcVaccineCode(
              dose,
              route,
              personId,
              visitOccId,
              immunizationOnset,
              immunizationLogicId,
              immunizationSourceIdentifier,
              atcCoding);
      vaccineDrugExposure.add(atcDrugExposure);
    }

    return vaccineDrugExposure;
  }

  /**
   * @param newDrugExposure
   * @param vaccineDrugExposure
   * @param atcCoding
   */
  private DrugExposure setAtcVaccineCode(
      Quantity dose,
      String route,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      Coding atcCoding) {
    var basisDrugExposure =
        setBasisDrugExposure(
            dose,
            route,
            personId,
            visitOccId,
            immunizationOnset,
            immunizationLogicId,
            immunizationSourceIdentifier);
    var atcConcept =
        findOmopConcepts.getConcepts(
            atcCoding, basisDrugExposure.getDrugExposureStartDate(), bulkload, dbMappings);
    if (atcConcept == null) {
      return null;
    }
    basisDrugExposure.setDrugConceptId(atcConcept.getConceptId());
    basisDrugExposure.setDrugSourceConceptId(atcConcept.getConceptId());
    basisDrugExposure.setDrugSourceValue(atcCoding.getCode());
    return basisDrugExposure;
  }

  /**
   * @param newDrugExposure
   * @param vaccineDrugExposure
   * @param snomedCoding
   */
  private List<DrugExposure> setSnomedVaccineCode(
      Quantity dose,
      String route,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      Coding snomedCoding) {

    List<DrugExposure> vaccineDrugExposure = new ArrayList<>();
    var snomedCodingList = getSnomedCodingList(snomedCoding);
    for (var subSnomedCoding : snomedCodingList) {

      var snomedVaccineConcepts =
          findOmopConcepts.getSnomedVaccineConcepts(
              subSnomedCoding,
              immunizationOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings);

      for (var snomedVaccineConcept : snomedVaccineConcepts) {
        var basisDrugExposure =
            setBasisDrugExposure(
                dose,
                route,
                personId,
                visitOccId,
                immunizationOnset,
                immunizationLogicId,
                immunizationSourceIdentifier);
        basisDrugExposure.setDrugConceptId(snomedVaccineConcept.getStandardVaccineConceptId());
        basisDrugExposure.setDrugSourceConceptId(snomedVaccineConcept.getSnomedConceptId());
        basisDrugExposure.setDrugSourceValue(snomedVaccineConcept.getSnomedCode());

        vaccineDrugExposure.add(basisDrugExposure);
      }
    }

    return vaccineDrugExposure;
  }

  private List<Coding> getSnomedCodingList(Coding snomedCoding) {
    var snomedCodes = snomedCoding.getCode();
    if (!snomedCodes.contains("+")) {
      return Arrays.asList(snomedCoding);
    }

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
  }

  private boolean checkIfCodeExists(Coding vaccineCoding) {
    if (vaccineCoding == null) {
      return false;
    }

    var code = checkDataAbsentReason.getValue(vaccineCoding.getCodeElement());
    if (Strings.isNullOrEmpty(code)) {
      return false;
    }
    return true;
  }

  private String getRoute(Immunization srcImmunization) {
    var routeCodeableConcept = checkDataAbsentReason.getValue(srcImmunization.getRoute());
    if (routeCodeableConcept == null) {
      return null;
    }
    var routeCodings = routeCodeableConcept.getCoding();
    var routeText = routeCodeableConcept.getText();
    if (routeCodings.isEmpty() && Strings.isNullOrEmpty(routeText)) {
      return null;
    } else if (routeCodings.isEmpty() && !Strings.isNullOrEmpty(routeText)) {
      return routeText;
    } else {
      return routeCodings.get(0).getCode();
    }
  }

  private Coding getCoding(List<Coding> vaccineCodingList, List<String> fhirSystemUrl) {
    var codingOptional =
        vaccineCodingList.stream()
            .filter(coding -> fhirSystemUrl.contains(coding.getSystem()))
            .findFirst();
    if (codingOptional.isPresent()) {
      return codingOptional.get();
    }
    return null;
  }

  private List<Coding> getVaccineCoding(Immunization srcImmunization) {
    var vaccineCodeableConcept = srcImmunization.getVaccineCode();
    var vaccinecodeableConceptValue = checkDataAbsentReason.getValue(vaccineCodeableConcept);
    if (vaccinecodeableConceptValue == null) {
      return Collections.emptyList();
    }

    if (vaccineCodeableConcept.isEmpty()) {
      return Collections.emptyList();
    }
    var vaccineCodings = vaccineCodeableConcept.getCoding();
    if (vaccineCodings.isEmpty()) {
      return Collections.emptyList();
    }

    List<Coding> vaccineCodingList = new ArrayList<>();
    for (var coding : vaccineCodings) {
      if (fhirSystems.getVaccineCode().contains(coding.getSystem())) {
        vaccineCodingList.add(coding);
      }
    }

    return vaccineCodingList;
  }

  private String getStatus(Immunization srcImmunization) {
    var statusElement = srcImmunization.getStatusElement();
    if (!statusElement.isEmpty()) {
      var status = checkDataAbsentReason.getValue(statusElement);
      if (status != null) {
        return status;
      }
    }
    return null;
  }

  private ResourceOnset getImmunizationOnset(Immunization srcImmunization) {
    var resourceOnset = new ResourceOnset();
    if (!srcImmunization.hasOccurrenceDateTimeType()) {
      return resourceOnset;
    }

    var immunizationOccurrenceDateTimeType = srcImmunization.getOccurrenceDateTimeType();
    if (!immunizationOccurrenceDateTimeType.isEmpty()) {
      var immunizationOccurrenceDateTime =
          checkDataAbsentReason.getValue(immunizationOccurrenceDateTimeType);
      if (immunizationOccurrenceDateTime != null) {
        resourceOnset.setStartDateTime(immunizationOccurrenceDateTime);
        resourceOnset.setEndDateTime(immunizationOccurrenceDateTime);
      }
    }

    return resourceOnset;
  }

  private Long getVisitOccId(
      Immunization srcImmunization, String immunizationLogicId, Long personId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcImmunization);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcImmunization);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            immunizationLogicId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for [Immunization]: {}.", immunizationLogicId);
    }

    return visitOccId;
  }

  private Long getPersonId(Immunization srcImmunization, String conditionLogicId) {
    var patientReferenceIdentifier =
        fhirReferenceUtils.getSubjectReferenceIdentifier(srcImmunization);
    var patientReferenceLogicalId =
        fhirReferenceUtils.getSubjectReferenceLogicalId(srcImmunization);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, conditionLogicId);
  }
}
