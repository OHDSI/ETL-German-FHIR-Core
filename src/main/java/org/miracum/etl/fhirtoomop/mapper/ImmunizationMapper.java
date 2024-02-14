package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_DRUG;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ATC;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_IPRD;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
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
import org.miracum.etl.fhirtoomop.model.AtcStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.SnomedVaccineStandardLookup;
import org.miracum.etl.fhirtoomop.model.omop.DrugExposure;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.repository.service.DrugExposureMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.ImmunizationMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ImmunizationMapper implements FhirMapper<Immunization> {
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final DbMappings dbMappings;
  private final Boolean bulkload;
  private final List<String> listOfImmunizationVocabularyId =
      Arrays.asList(VOCABULARY_ATC, VOCABULARY_SNOMED, VOCABULARY_IPRD);

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ImmunizationMapperServiceImpl immunizationService;
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
//    var result = Objects.equals(immunizationLogicId, "imm-02769f55-7a3b-49f5-8d96-2c2e07394c13");
//    if(!result){
//      return null;
//    }
    var immunizationSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcImmunization);
    if (Strings.isNullOrEmpty(immunizationLogicId)
        && Strings.isNullOrEmpty(immunizationSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Immunization] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }

    String immunizationId = "";
    if (!Strings.isNullOrEmpty(immunizationLogicId)) {
      immunizationId = srcImmunization.getId();
    }

    if (Boolean.FALSE.equals(bulkload)) {
      deleteExistingImmunization(immunizationLogicId, immunizationSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info(
            "Found a deleted [Immunization] resource {}. Deleting from OMOP DB.", immunizationId);
        return null;
      }
    }

    var status = getStatus(srcImmunization);
    if (status == null || !FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST.contains(status)) {
      log.error(
          "The [status]: {} of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          status,
          immunizationId);
      return null;
    }

    var personId = getPersonId(srcImmunization, immunizationLogicId, immunizationId);
    if (personId == null) {
      log.warn("No matching [Person] found for [Immunization]: {}. Skip resource", immunizationId);
      noPersonIdCounter.increment();
      return null;
    }

    var immunizationOnset = getImmunizationOnset(srcImmunization);
    if (immunizationOnset.getStartDateTime() == null) {
      log.warn("No [Date] found for [Immunization]: {}. Skip resource", immunizationId);
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
      log.error("No [vaccine code] found for [Immunization]: {}. Skip resource.", immunizationId);
      noCodeCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcImmunization, personId, immunizationId);
    var route = getRoute(srcImmunization);
    var dose = getDose(srcImmunization);

    createImmunizationMapping(
        wrapper,
        dose,
        route,
        vaccineCodingList,
        personId,
        visitOccId,
        immunizationOnset,
        immunizationLogicId,
        immunizationSourceIdentifier,
        immunizationId);

    return wrapper;
  }

  private Quantity getDose(Immunization srcImmunization) {
    return srcImmunization.getDoseQuantity();
  }

  private void deleteExistingImmunization(
      String immunizationLogicId, String immunizationSourceIdentifier) {
    if (!Strings.isNullOrEmpty(immunizationLogicId)) {
      immunizationService.deleteExistingImmunizationByFhirLogicalId(immunizationLogicId);
    } else {
      immunizationService.deleteExistingImmunizationByFhirIdentifier(immunizationSourceIdentifier);
    }
  }

  private void createImmunizationMapping(
      OmopModelWrapper wrapper,
      Quantity dose,
      Coding route,
      List<Coding> vaccineCodingList,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      String immunizationId) {
    var codingSize = vaccineCodingList.size();
    if (codingSize == 1) {
      setImmunizationConceptsUsingSingleCoding(
          wrapper,
          dose,
          route,
          vaccineCodingList.get(0),
          personId,
          visitOccId,
          immunizationOnset,
          immunizationLogicId,
          immunizationSourceIdentifier,
          immunizationId);
    } else {
      setImmunizationConceptsUsingMultipleCodings(
          wrapper,
          dose,
          route,
          vaccineCodingList,
          personId,
          visitOccId,
          immunizationOnset,
          immunizationLogicId,
          immunizationSourceIdentifier,
          immunizationId);
    }
  }

  private void setImmunizationConceptsUsingSingleCoding(
      OmopModelWrapper wrapper,
      Quantity dose,
      Coding route,
      Coding vaccineCoding,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      String immunizationId) {

    List<Pair<String, List<AtcStandardDomainLookup>>> atcStandardMapPairList = null;

    var immunizationCodeExist =
        checkIfAnyImmunizationCodesExist(vaccineCoding, listOfImmunizationVocabularyId);
    if (!immunizationCodeExist) {
      return;
    }

    var immunizationVocabularyId = findOmopConcepts.getOmopVocabularyId(vaccineCoding.getSystem());

    if (immunizationVocabularyId.equals(VOCABULARY_ATC)) {
      // for ATC codes

      atcStandardMapPairList =
          getValidAtcCodes(
              vaccineCoding, immunizationOnset.getStartDateTime().toLocalDate(), immunizationId);

      if (atcStandardMapPairList.isEmpty()) {
        return;
      }
      for (var singlePair : atcStandardMapPairList) {
        immunizationProcessor(
            singlePair,
            Collections.emptyList(),
            wrapper,
            dose,
            route,
            immunizationOnset,
            immunizationLogicId,
            immunizationSourceIdentifier,
            personId,
            visitOccId,
            immunizationId);
      }
    } else if (immunizationVocabularyId.equals(VOCABULARY_SNOMED)) {
      // for SNOMED codes

      var snomedCodingList = getSnomedCodingList(vaccineCoding);
      var snomedStandardConcepts =
          getSnomedConceptList(
              snomedCodingList, immunizationOnset, immunizationLogicId, immunizationId);

      immunizationProcessor(
          null,
          snomedStandardConcepts,
          wrapper,
          dose,
          route,
          immunizationOnset,
          immunizationLogicId,
          immunizationSourceIdentifier,
          personId,
          visitOccId,
          immunizationId);
    }else if (immunizationVocabularyId.equals(VOCABULARY_IPRD)) {
      // for SNOMED codes

      var snomedCodingList = getSnomedCodingList(vaccineCoding);
      var snomedStandardConcepts =
              getSnomedConceptList(
                      snomedCodingList, immunizationOnset, immunizationLogicId, immunizationId);

      immunizationProcessor(
              null,
              snomedStandardConcepts,
              wrapper,
              dose,
              route,
              immunizationOnset,
              immunizationLogicId,
              immunizationSourceIdentifier,
              personId,
              visitOccId,
              immunizationId);
    }
  }

  private void setImmunizationConceptsUsingMultipleCodings(
      OmopModelWrapper wrapper,
      Quantity dose,
      Coding route,
      List<Coding> vaccineCodings,
      Long personId,
      Long visitOccId,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      String immunizationId) {

    Coding uncheckedAtcCoding = null;
    Coding uncheckedSnomedCoding = null;
    Coding immunizationCoding = null;

    for (var uncheckedCoding : vaccineCodings) {
      var immunizationVocabularyId =
          findOmopConcepts.getOmopVocabularyId(uncheckedCoding.getSystem());
      if (immunizationVocabularyId.equals(VOCABULARY_ATC)) {
        uncheckedAtcCoding = uncheckedCoding;
      }
      if (immunizationVocabularyId.equals(VOCABULARY_SNOMED)) {
        uncheckedSnomedCoding = uncheckedCoding;
      }
    }
    if (uncheckedAtcCoding == null && uncheckedSnomedCoding == null) {
      return;
    }

    // ATC
    var atcStandardMapPairList =
        getValidAtcCodes(
            uncheckedAtcCoding, immunizationOnset.getStartDateTime().toLocalDate(), immunizationId);

    // SNOMED
    var snomedCodingList = getSnomedCodingList(uncheckedSnomedCoding);
    var snomedStandardConcepts =
        getSnomedConceptList(
            snomedCodingList, immunizationOnset, immunizationLogicId, immunizationId);

    if (atcStandardMapPairList.isEmpty() && snomedStandardConcepts.isEmpty()) {
      return;
    } else if (!atcStandardMapPairList.isEmpty()) {
      // ATC
      immunizationCoding = uncheckedAtcCoding;
    } else if (!snomedStandardConcepts.isEmpty()) {
      // SNOMED
      immunizationCoding = uncheckedSnomedCoding;
    }

    setImmunizationConceptsUsingSingleCoding(
        wrapper,
        dose,
        route,
        immunizationCoding,
        personId,
        visitOccId,
        immunizationOnset,
        immunizationLogicId,
        immunizationSourceIdentifier,
        immunizationId);
  }

  private List<SnomedVaccineStandardLookup> getSnomedConceptList(
      List<Coding> snomedCodingList,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationId) {
    List<SnomedVaccineStandardLookup> snomedStandardConcepts = new ArrayList<>();

    for (var subSnomedCoding : snomedCodingList) {
      var conceptList =
          findOmopConcepts.getSnomedVaccineConcepts(
              subSnomedCoding,
              immunizationOnset.getStartDateTime().toLocalDate(),
              bulkload,
              dbMappings,
              immunizationLogicId,
              immunizationId);
      if (conceptList.isEmpty()) {
        return Collections.emptyList();
      }
      snomedStandardConcepts.addAll(conceptList);
    }
    return snomedStandardConcepts;
  }

  /**
   * Extract valid pairs of ATC code and its OMOP concept_id and domain information as a list
   *
   * @param atcCoding
   * @param immunizationDate the date of immunization
   * @return a list of valid pairs of ATC code and its OMOP concept_id and domain information
   */
  private List<Pair<String, List<AtcStandardDomainLookup>>> getValidAtcCodes(
      Coding atcCoding, LocalDate immunizationDate, String immunizationId) {
    if (atcCoding == null) {
      return Collections.emptyList();
    }

    List<Pair<String, List<AtcStandardDomainLookup>>> validAtcStandardConceptMaps =
        new ArrayList<>();
    List<AtcStandardDomainLookup> atcStandardMap =
        findOmopConcepts.getAtcStandardConcepts(
            atcCoding, immunizationDate, bulkload, dbMappings, immunizationId);
    if (atcStandardMap.isEmpty()) {
      return Collections.emptyList();
    }

    validAtcStandardConceptMaps.add(Pair.of(atcCoding.getCode(), atcStandardMap));

    return validAtcStandardConceptMaps;
  }

  /**
   * Processes information from FHIR Immunization resource and transforms them into records OMOP CDM
   * tables.
   */
  private void immunizationProcessor(
      @Nullable Pair<String, List<AtcStandardDomainLookup>> atcStandardPair,
      List<SnomedVaccineStandardLookup> snomedVaccineList,
      OmopModelWrapper wrapper,
      Quantity dose,
      Coding route,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      Long personId,
      Long visitOccId,
      String immunizationId) {

    if (atcStandardPair == null && snomedVaccineList.isEmpty()) {
      return;
    }

    if (atcStandardPair != null) {
      var atcCode = atcStandardPair.getLeft();
      var atcStandardMaps = atcStandardPair.getRight();

      for (var atcStandardMap : atcStandardMaps) {
        setImmunization(
            wrapper,
            dose,
            route,
            immunizationOnset,
            immunizationLogicId,
            immunizationSourceIdentifier,
            personId,
            visitOccId,
            atcCode,
            atcStandardMap.getStandardConceptId(),
            atcStandardMap.getSourceConceptId(),
            atcStandardMap.getStandardDomainId(),
            immunizationId);
      }

    } else {
      for (var snomedVaccine : snomedVaccineList) {
        setImmunization(
            wrapper,
            dose,
            route,
            immunizationOnset,
            immunizationLogicId,
            immunizationSourceIdentifier,
            personId,
            visitOccId,
            snomedVaccine.getSnomedCode(),
            snomedVaccine.getStandardVaccineConceptId(),
            snomedVaccine.getSnomedConceptId(),
            snomedVaccine.getStandardVaccineDomainId(),
            immunizationId);
      }
    }
  }

  /** Write immunization information into correct OMOP tables based on their domains. */
  private void setImmunization(
      OmopModelWrapper wrapper,
      Quantity dose,
      Coding route,
      ResourceOnset immunizationOnset,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      Long personId,
      Long visitOccId,
      String immunizationCode,
      Integer immunizationConceptId,
      Integer immunizationSourceConceptId,
      String domain,
      String immunizationId) {
    if(domain == null){
      log.warn("fhirId = {}={}",immunizationId,domain);
      return;
    }
    switch (domain) {
      case OMOP_DOMAIN_DRUG:
        var drug =
            setUpDrugExposure(
                dose,
                route,
                immunizationOnset,
                immunizationConceptId,
                immunizationSourceConceptId,
                immunizationCode,
                personId,
                visitOccId,
                immunizationLogicId,
                immunizationSourceIdentifier,
                immunizationId);

        wrapper.getDrugExposure().add(drug);

        break;
      case OMOP_DOMAIN_OBSERVATION:
        var observation =
            setUpObservation(
                dose,
                route,
                immunizationOnset,
                immunizationConceptId,
                immunizationSourceConceptId,
                immunizationCode,
                personId,
                visitOccId,
                immunizationLogicId,
                immunizationSourceIdentifier,
                immunizationId);

        wrapper.getObservation().add(observation);

        break;
      default:
        log.error(
            "[Unsupported domain] {} of code in [Immunization]: {}. Skip resource.",
            domain,
            immunizationId);
        break;
    }
  }

  /**
   * Creates a new record of the drug_exposure table in OMOP CDM for the processed FHIR Immunization
   * resource.
   *
   * @return new record of the drug_exposure table in OMOP CDM for the processed FHIR Immunization
   *     resource
   */
  private DrugExposure setUpDrugExposure(
      Quantity dose,
      Coding route,
      ResourceOnset onset,
      Integer immunizationConceptId,
      Integer immunizationSourceConceptId,
      String immunizationCode,
      Long personId,
      Long visitOccId,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      String immunizationId) {

    var startDateTime = onset.getStartDateTime();
    var endDateTime = onset.getEndDateTime();

    var drugExposure =
        DrugExposure.builder()
            .drugExposureStartDate(startDateTime.toLocalDate())
            .drugExposureStartDatetime(startDateTime)
            .drugExposureEndDatetime(endDateTime)
            .drugExposureEndDate(
                endDateTime == null ? startDateTime.toLocalDate() : endDateTime.toLocalDate())
            .personId(personId)
            .drugSourceConceptId(immunizationSourceConceptId)
            .drugConceptId(immunizationConceptId)
            .visitOccurrenceId(visitOccId)
            .drugTypeConceptId(CONCEPT_EHR)
            .drugSourceValue(immunizationCode)
            .fhirLogicalId(immunizationLogicId)
            .fhirIdentifier(immunizationSourceIdentifier)
            .build();

    if (dose != null) {
      drugExposure.setDoseUnitSourceValue(dose.getUnit());
      drugExposure.setQuantity(dose.getValue());
    }

    if (route != null) {
      var routeConcept =
          findOmopConcepts.getConcepts(
              route, startDateTime.toLocalDate(), bulkload, dbMappings, immunizationId);
      drugExposure.setRouteSourceValue(routeConcept.getConceptCode());
      drugExposure.setRouteConceptId(
          routeConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
              ? null
              : routeConcept.getConceptId());
    }

    return drugExposure;
  }

  private OmopObservation setUpObservation(
      Quantity dose,
      Coding route,
      ResourceOnset onset,
      Integer immunizationConceptId,
      Integer immunizationSourceConceptId,
      String immunizationCode,
      Long personId,
      Long visitOccId,
      String immunizationLogicId,
      String immunizationSourceIdentifier,
      String immunizationId) {

    var startDateTime = onset.getStartDateTime();

    var newObservation =
        OmopObservation.builder()
            .personId(personId)
            .observationDate(startDateTime.toLocalDate())
            .observationDatetime(startDateTime)
            .visitOccurrenceId(visitOccId)
            .observationSourceConceptId(immunizationSourceConceptId)
            .observationConceptId(immunizationConceptId)
            .observationTypeConceptId(CONCEPT_EHR)
            .observationSourceValue(immunizationCode)
            .fhirLogicalId(immunizationLogicId)
            .fhirIdentifier(immunizationSourceIdentifier)
            .build();

    if (dose != null) {
      var doseCoding = new Coding().setCode(dose.getUnit()).setSystem(fhirSystems.getUcum());

      var unitConcept =
          findOmopConcepts.getConcepts(
              doseCoding, startDateTime.toLocalDate(), bulkload, dbMappings, immunizationId);

      newObservation.setUnitSourceValue(dose.getUnit());
      newObservation.setValueAsNumber(dose.getValue());
      newObservation.setUnitConceptId(
          unitConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
              ? null
              : unitConcept.getConceptId());
    }

    if (route != null) {
      var routeConcept =
          findOmopConcepts.getConcepts(
              route, startDateTime.toLocalDate(), bulkload, dbMappings, immunizationId);
      newObservation.setQualifierSourceValue(routeConcept.getConceptCode());
      newObservation.setQualifierConceptId(
          routeConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
              ? null
              : routeConcept.getConceptId());
    }

    return newObservation;
  }

  private List<Coding> getSnomedCodingList(Coding snomedCoding) {
    if (snomedCoding == null) {
      return Collections.emptyList();
    }
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

  private Coding getRoute(Immunization srcImmunization) {
    var routeCodeableConcept = checkDataAbsentReason.getValue(srcImmunization.getRoute());
    if (routeCodeableConcept == null) {
      return null;
    }
    var routeCodings = routeCodeableConcept.getCoding();
    if (routeCodings.isEmpty()) {
      return null;
    } else {
      return routeCodings.get(0);
    }
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

  private Long getVisitOccId(Immunization srcImmunization, Long personId, String immunizationId) {
    var encounterReferenceIdentifier =
        fhirReferenceUtils.getEncounterReferenceIdentifier(srcImmunization);
    var encounterReferenceLogicalId =
        fhirReferenceUtils.getEncounterReferenceLogicalId(srcImmunization);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier, encounterReferenceLogicalId, personId, immunizationId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for [Immunization]: {}.", immunizationId);
    }

    return visitOccId;
  }

  private Long getPersonId(
      Immunization srcImmunization, String immunizationLogicId, String immunizationId) {
    var patientReferenceIdentifier =
        fhirReferenceUtils.getSubjectReferenceIdentifier(srcImmunization);
    var patientReferenceLogicalId =
        fhirReferenceUtils.getSubjectReferenceLogicalId(srcImmunization);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, immunizationLogicId, immunizationId);
  }

  /**
   * Check if the used vaccine code exists in OMOP
   *
   * @param vaccineCoding Coding element from Immunization FHIR resource
   * @param vocabularyId vocabulary Id in OMOP based on the used system URL in Coding
   * @return a boolean value
   */
  private boolean checkIfAnyImmunizationCodesExist(
      Coding vaccineCoding, List<String> vocabularyId) {
    if (vaccineCoding == null) {
      return false;
    }
    var codingVocabularyId = findOmopConcepts.getOmopVocabularyId(vaccineCoding.getSystem());
    return vocabularyId.contains(codingVocabularyId);
  }
}
