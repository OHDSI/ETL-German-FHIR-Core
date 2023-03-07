package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_GECCO_OBSERVATION_ACCEPTABLE_VALUE_CODE;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_GECCO_OBSERVATION_BLOOD_PRESSURE_CODES;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_GECCO_OBSERVATION_ECRF_PARAMETER_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_GECCO_OBSERVATION_IN_MEASUREMENT_DOMAIN_CODES;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_GECCO_OBSERVATION_SOFA_CODES;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_OBSERVATION_ACCEPTABLE_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_OBSERVATION_HISTORY_OF_TRAVEL_CODES;
import static org.miracum.etl.fhirtoomop.Constants.MAX_SOURCE_VALUE_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_GENDER;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_MEASUREMENT;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_FRAILTY_SCORE;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_ECRF_PARAMETER;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_GENDER;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_LAB_INTERPRETATION;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_LAB_RESULT;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_SOFA_CATEGORY;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Observation.ObservationReferenceRangeComponent;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Type;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.Concept.Domain;
import org.miracum.etl.fhirtoomop.model.omop.Measurement;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.repository.service.ObservationMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The ObservationMapper class describes the business logic of transforming a FHIR Observation
 * resource to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class ObservationMapper implements FhirMapper<Observation> {

  private static final FhirSystems fhirSystems = new FhirSystems();
  private final ResourceFhirReferenceUtils referenceUtils;
  private final Boolean bulkload;
  private final DbMappings dbMappings;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ObservationMapperServiceImpl observationService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessObservations");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessObservations");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessObservations");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessObservations");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessObservations");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessObservations");

  /**
   * Constructor for objects of the class ObservationMapper.
   *
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public ObservationMapper(
      ResourceFhirReferenceUtils referenceUtils, Boolean bulkload, DbMappings dbMappings) {

    this.referenceUtils = referenceUtils;
    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR Observation resource to several OMOP CDM tables.
   *
   * @param srcObservation FHIR Observation resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Observation
   *     resource
   */
  @Override
  public OmopModelWrapper map(Observation srcObservation, boolean isDeleted) {
    var wrapper = new OmopModelWrapper();

    var observationLogicId = fhirReferenceUtils.extractId(srcObservation);
    var observationSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcObservation);
    if (StringUtils.isBlank(observationLogicId)
        && StringUtils.isBlank(observationSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Observation] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }

    var statusElement = srcObservation.getStatusElement();
    var statusValue = checkDataAbsentReason.getValue(statusElement);
    if (Strings.isNullOrEmpty(statusValue)
        || !FHIR_RESOURCE_OBSERVATION_ACCEPTABLE_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status] of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          observationLogicId);
      return null;
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingLabObservations(observationLogicId, observationSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", observationLogicId);
        return null;
      }
    }

    var personId = getPersonId(srcObservation, observationLogicId);
    if (personId == null) {
      log.warn("No matching [Person] found for {}. Skip resource", observationLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcObservation, personId, observationLogicId);

    var observationCoding = getObservationCoding(srcObservation);
    if (observationCoding == null) {
      log.warn("No Code found in [Observation]:{}. Skip resource", observationLogicId);
      noCodeCounter.increment();
      return null;
    }

    var effectiveDateTime = getObservationOnset(srcObservation);
    if (effectiveDateTime.getStartDateTime() == null
        && !FHIR_RESOURCE_OBSERVATION_HISTORY_OF_TRAVEL_CODES.contains(
            observationCoding.getCode())) {
      log.warn("No [EffectiveDateTime] found for {}. Skip resource", observationLogicId);
      noStartDateCounter.increment();
      return null;
    }

    setLabData(
        wrapper,
        observationCoding,
        srcObservation,
        personId,
        visitOccId,
        effectiveDateTime.getStartDateTime(),
        observationLogicId,
        observationSourceIdentifier);

    return wrapper;
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR
   * Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Observation srcObservation, String observationLogicId) {
    var patientReferenceIdentifier = referenceUtils.getSubjectReferenceIdentifier(srcObservation);
    var patientReferenceLogicalId = referenceUtils.getSubjectReferenceLogicalId(srcObservation);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, observationLogicId);
  }

  /**
   * Returns the visit_occurrence_id of the referenced FHIR Encounter resource for the processed
   * FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(Observation srcObservation, Long personId, String observationLogicId) {

    var encounterReferenceIdentifier =
        referenceUtils.getEncounterReferenceIdentifier(srcObservation);
    var encounterReferenceLogicalId = referenceUtils.getEncounterReferenceLogicalId(srcObservation);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            observationLogicId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for {}.", observationLogicId);
    }

    return visitOccId;
  }

  /**
   * Extracts date time information from the FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @return date time of the FHIR Observation resource
   */
  private ResourceOnset getObservationOnset(Observation srcObservation) {
    var resourceOnset = new ResourceOnset();
    if (srcObservation.hasEffective()) {
      var effective = checkDataAbsentReason.getValue(srcObservation.getEffective());
      if (effective == null) {
        return resourceOnset;
      }
    }

    if (srcObservation.hasEffectiveDateTimeType()) {
      var effectiveDateTime =
          checkDataAbsentReason.getValue(srcObservation.getEffectiveDateTimeType());
      if (effectiveDateTime != null) {
        resourceOnset.setStartDateTime(effectiveDateTime);
      }
      return resourceOnset;
    }

    if (srcObservation.hasIssued() && srcObservation.getIssued() != null) {
      resourceOnset.setStartDateTime(
          new Timestamp(srcObservation.getIssued().getTime()).toLocalDateTime());
      return resourceOnset;
    }

    if (srcObservation.hasEffectivePeriod()) {
      var effectivePeriod = srcObservation.getEffectivePeriod();
      if (effectivePeriod.getStart() != null) {
        resourceOnset.setStartDateTime(
            new Timestamp(srcObservation.getEffectivePeriod().getStart().getTime())
                .toLocalDateTime());
      }
      if (effectivePeriod.getEnd() != null) {
        resourceOnset.setEndDateTime(
            new Timestamp(srcObservation.getEffectivePeriod().getEnd().getTime())
                .toLocalDateTime());
      }
      return resourceOnset;
    }

    return resourceOnset;
  }

  private void setHistoryOfTravelInObservation(
      OmopModelWrapper wrapper,
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      String observationLogicId,
      String observationSourceIdentifier) {
    // Only for GECCO History-of-travel profile
    var valueCodeableConcept = getValueCodeableConcept(srcObservation);
    if (valueCodeableConcept == null) {
      log.debug(
          "No result found in Observation for History-of-travel [{}]. Skip resource.",
          observationLogicId);
      return;
    }
    var value =
        valueCodeableConcept.getCoding().stream()
            .filter(
                coding ->
                    FHIR_RESOURCE_GECCO_OBSERVATION_ACCEPTABLE_VALUE_CODE.contains(
                        coding.getCode()))
            .findFirst();
    if (!value.isPresent()) {
      log.debug(
          "No acceptable result found in Observation for History-of-travel [{}]. Skip resource.",
          observationLogicId);
      return;
    }
    if (!srcObservation.hasComponent()) {
      log.debug(
          "No available information found in Observation for History-of-travel [{}]. Skip resource.",
          observationLogicId);
      return;
    }

    var componentList = srcObservation.getComponent();
    if (componentList.isEmpty()) {
      log.debug(
          "No available information found in Observation for History-of-travel [{}]. Skip resource.",
          observationLogicId);
      return;
    }

    var travelComponentStartedDate = getComponentElement(componentList, "82752-7");
    setDateTimeTypeInObservation(
        wrapper,
        srcObservation,
        personId,
        visitOccId,
        observationLogicId,
        observationSourceIdentifier,
        travelComponentStartedDate);
    var travelComponentEndedDate = getComponentElement(componentList, "91560-3");
    setDateTimeTypeInObservation(
        wrapper,
        srcObservation,
        personId,
        visitOccId,
        observationLogicId,
        observationSourceIdentifier,
        travelComponentEndedDate);

    StringBuilder dataOne = new StringBuilder();
    StringBuilder dataTwo = new StringBuilder();
    dataOne.append(";");
    dataTwo.append(";");
    var travelComponentCity = getComponentElement(componentList, "94653-3");
    if (travelComponentCity != null) {
      dataOne.append(
          travelComponentCity
                  .getRight()
                  .castToString(travelComponentCity.getRight())
                  .asStringValue()
              + ";");
    }

    var travelComponentCountry = getComponentElement(componentList, "94651-7");
    if (travelComponentCountry != null) {
      dataOne.append(
          travelComponentCountry
              .getRight()
              .castToCodeableConcept(travelComponentCountry.getRight())
              .getCodingFirstRep()
              .getCode());
    }

    var travelComponentState = getComponentElement(componentList, "82754-3");
    if (travelComponentState != null) {
      dataTwo.append(
          travelComponentState
              .getRight()
              .castToCodeableConcept(travelComponentState.getRight())
              .getCodingFirstRep()
              .getCode());
    }
    if (dataOne.toString().equals(";") && dataTwo.toString().equals(";")) {
      return;
    }

    var ppm =
        PostProcessMap.builder()
            .type(ResourceType.OBSERVATION.name())
            .omopTable(OmopModelWrapper.Tablename.LOCATION.name())
            .fhirIdentifier(observationSourceIdentifier)
            .fhirLogicalId(observationLogicId)
            .dataOne(dataOne.toString())
            .dataTwo(dataTwo.toString())
            .build();

    wrapper.getPostProcessMap().add(ppm);
  }

  /**
   * @param wrapper
   * @param srcObservation
   * @param personId
   * @param visitOccId
   * @param observationLogicId
   * @param observationSourceIdentifier
   * @param dateTravelStartedComponent
   */
  private void setDateTimeTypeInObservation(
      OmopModelWrapper wrapper,
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      String observationLogicId,
      String observationSourceIdentifier,
      Pair<Coding, Type> dateTravelDateComponent) {
    if (dateTravelDateComponent == null) {
      return;
    }
    var travelDate =
        new Timestamp(
                dateTravelDateComponent
                    .getRight()
                    .castToDateTime(dateTravelDateComponent.getRight())
                    .getValue()
                    .getTime())
            .toLocalDateTime();
    var travelDateConcept =
        findOmopConcepts.getConcepts(
            dateTravelDateComponent.getLeft(), travelDate.toLocalDate(), bulkload, dbMappings);
    if (travelDateConcept == null) {
      return;
    }
    wrapper
        .getObservation()
        .add(
            createBasisObservation(
                srcObservation,
                personId,
                visitOccId,
                travelDateConcept,
                travelDate,
                observationLogicId,
                observationSourceIdentifier));
  }

  private Pair<Coding, Type> getComponentElement(
      List<ObservationComponentComponent> componentList, String componentCode) {
    if (componentList.isEmpty()) {
      return null;
    }
    for (var component : componentList) {
      var componentCoding = getComponentElementCoding(component, componentCode);
      if (componentCoding != null && component.getValue() != null) {
        return Pair.of(componentCoding, component.getValue());
      }
    }
    return null;
  }

  private Coding getComponentElementCoding(
      ObservationComponentComponent component, String componentCode) {
    var componentElementOptinal =
        component.getCode().getCoding().stream()
            .filter(coding -> coding.getCode().equals(componentCode))
            .findFirst();
    if (componentElementOptinal.isPresent()) {
      return componentElementOptinal.get();
    }
    return null;
  }

  /**
   * Extract code for laboratory analysis from Observation FHIR resource
   *
   * @param srcObservation FHIR Observation resource
   * @return code for laboratory analysis
   */
  private Coding getObservationCoding(Observation srcObservation) {
    if (!srcObservation.hasCode()) {
      return null;
    }

    var observationCodeableConcept = checkDataAbsentReason.getValue(srcObservation.getCode());
    if (observationCodeableConcept == null) {
      return null;
    }
    var observation =
        observationCodeableConcept.getCoding().stream()
            .filter(code -> fhirSystems.getObservationCode().contains(code.getSystem()))
            .findFirst();

    if (!observation.isPresent()) {
      return null;
    }
    return observation.get();
  }

  /**
   * Maps the processed FHIR Observation resource with lab data to OMOP CDM tables.
   *
   * @param wrapper cache of newly created OMOP CDM records from the FHIR Observation resource
   * @param observationCoding laboratory analysis code from the FHIR Observation resource
   * @param srcObservation FHIR Observation resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @param observationSourceIdentifier identifier of the FHIR Observation resource
   */
  private void setLabData(
      OmopModelWrapper wrapper,
      Coding observationCoding,
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier) {
    Concept observationCodeConcept = null;

    if (FHIR_RESOURCE_GECCO_OBSERVATION_ECRF_PARAMETER_DOMAIN_OBSERVATION.contains(
        observationCoding.getCode())) {
      observationCodeConcept =
          createGeccoCustomConcept(
              observationCoding, SOURCE_VOCABULARY_ID_ECRF_PARAMETER, OMOP_DOMAIN_OBSERVATION);
    } else if (FHIR_RESOURCE_GECCO_OBSERVATION_SOFA_CODES.contains(observationCoding.getCode())) {
      observationCodeConcept =
          createGeccoCustomConcept(
              observationCoding, SOURCE_VOCABULARY_ID_ECRF_PARAMETER, OMOP_DOMAIN_MEASUREMENT);
    } else {
      observationCodeConcept =
          findOmopConcepts.getConcepts(
              observationCoding, effectiveDateTime.toLocalDate(), bulkload, dbMappings);
    }

    if (observationCodeConcept == null) {
      return;
    }

    String domainId = observationCodeConcept.getDomainId();

    if (domainId.equals(Domain.OBSERVATION.getLabel())) {
      //      var observation =
      setObservation(
          srcObservation,
          personId,
          visitOccId,
          observationCodeConcept,
          effectiveDateTime,
          observationLogicId,
          observationSourceIdentifier,
          wrapper);
      //      wrapper.setObservation(observation);

    } else {
      //      var measurement =
      setMeasurement(
          srcObservation,
          personId,
          visitOccId,
          observationCodeConcept,
          effectiveDateTime,
          observationLogicId,
          observationSourceIdentifier,
          wrapper);

      //      wrapper.setMeasurement(measurement);
    }
  }

  /**
   * @param observationCoding
   * @return
   */
  private Concept createGeccoCustomConcept(
      Coding observationCoding, String vocabularyId, String domainId) {
    Concept observationCodeConcept;
    var geccoCustomConcept =
        findOmopConcepts.getCustomConcepts(observationCoding, vocabularyId, dbMappings);
    observationCodeConcept =
        Concept.builder()
            .conceptCode(observationCoding.getCode())
            .conceptId(geccoCustomConcept.getTargetConceptId())
            .domainId(domainId)
            .build();
    return observationCodeConcept;
  }

  /**
   * Creates new records of the observation table in OMOP CDM for the processed FHIR Observation
   * resource. The extracted LOINC code of the FHIR Observation resource belongs to the Observation
   * domain in OMOP CDM.
   *
   * @param srcObservation FHIR Observation resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param labObservationConcept concept of the laboratory analysis code from the FHIR Observation
   *     resource
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @param observationSourceIdentifier identifier of the FHIR Observation resource
   * @return list of new records of the observation table in OMOP CDM for the processed FHIR
   *     Observation resource
   */
  private void setObservation(
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier,
      OmopModelWrapper wrapper) {
    List<OmopObservation> observations = new ArrayList<>();

    if (FHIR_RESOURCE_OBSERVATION_HISTORY_OF_TRAVEL_CODES.contains(
        observationCodeConcept.getConceptCode())) {
      setHistoryOfTravelInObservation(
          wrapper,
          srcObservation,
          personId,
          visitOccId,
          observationLogicId,
          observationSourceIdentifier);
      return;
    }

    var basisObservation =
        createBasisObservation(
            srcObservation,
            personId,
            visitOccId,
            observationCodeConcept,
            effectiveDateTime,
            observationLogicId,
            observationSourceIdentifier);
    var valueQuantity = getValueQuantity(srcObservation);
    var valueCodeableConcept = getValueCodeableConcept(srcObservation);

    if (valueQuantity == null && valueCodeableConcept == null) {
      log.debug(
          "No [ValueQuantity] or [ValueCodeableConcept] found for [Observation]:{}. Skip.",
          observationLogicId);
      //      return Collections.emptyList();
      return;
    }
    if (valueQuantity != null) {
      setValueQuantityInObservation(
          effectiveDateTime, observations, basisObservation, valueQuantity);
    } else {
      setValueCodeableConceptInObservation(
          effectiveDateTime, observations, basisObservation, valueCodeableConcept);
    }

    //    addToList(observations, basisObservation);
    //    return observations;
    wrapper.setObservation(observations);
  }

  /**
   * Set value as quantity for OMOP OBSERVATION entry
   *
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param basisObservation the basic OBSERVATION entry
   * @param valueQuantity value from Observation FHIR resource as quantity
   */
  private void setValueQuantityInObservation(
      LocalDateTime effectiveDateTime,
      List<OmopObservation> observations,
      OmopObservation basisObservation,
      Quantity valueQuantity) {
    basisObservation.setValueAsNumber(valueQuantity.getValue());
    var quantityUnitCodingFormat =
        new Coding().setCode(valueQuantity.getCode()).setSystem(valueQuantity.getSystem());
    var valueQuantityUnitConcept =
        findOmopConcepts.getConcepts(
            quantityUnitCodingFormat, effectiveDateTime.toLocalDate(), bulkload, dbMappings);
    if (valueQuantityUnitConcept != null) {

      basisObservation.setUnitConceptId(valueQuantityUnitConcept.getConceptId());
      basisObservation.setUnitSourceValue(
          valueQuantity.getUnit() == null
              ? valueQuantityUnitConcept.getConceptCode()
              : valueQuantity.getUnit());
      addToList(observations, basisObservation);
    }
  }

  /**
   * Set value as code for OMOP OBSERVATION entry
   *
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param observations a list of OMOP OBSERVATION entries
   * @param basisObservation the basic OBSERVATION entry
   * @param valueCodeableConcept value from Observation FHIR resource as code
   */
  private void setValueCodeableConceptInObservation(
      LocalDateTime effectiveDateTime,
      List<OmopObservation> observations,
      OmopObservation basisObservation,
      CodeableConcept valueCodeableConcept) {
    if (valueCodeableConcept.isEmpty()) {
      return;
    }
    if (valueCodeableConcept.getCoding().isEmpty()) {

      var valueConcept =
          findOmopConcepts.getCustomConcepts(
              new Coding(null, valueCodeableConcept.getText(), null),
              SOURCE_VOCABULARY_ID_LAB_RESULT,
              dbMappings);

      basisObservation.setValueAsConceptId(valueConcept.getTargetConceptId());
      basisObservation.setValueAsString(valueConcept.getSourceCode());
    } else {
      for (var coding : valueCodeableConcept.getCoding()) {
        var system = coding.getSystem();
        Concept valueCodingConcept = null;
        if (fhirSystems.getGeccoBiologicalSex().contains(system)) {
          valueCodingConcept =
              createGeccoCustomConcept(coding, SOURCE_VOCABULARY_ID_GENDER, OMOP_DOMAIN_GENDER);
        } else {
          valueCodingConcept =
              findOmopConcepts.getConcepts(
                  coding, effectiveDateTime.toLocalDate(), bulkload, dbMappings);
        }

        if (valueCodingConcept != null) {
          basisObservation.setValueAsConceptId(valueCodingConcept.getConceptId());
          basisObservation.setValueAsString(valueCodingConcept.getConceptCode());
          addToList(observations, basisObservation);
        }
      }
    }
  }

  /**
   * Add new OMOP OBSERVATION entry to a list of OBSERVATION entries without duplicates
   *
   * @param omopObservations a list of OBSERVATION entries
   * @param omopObservation new OMOP OBSERVATION entry
   */
  private void addToList(List<OmopObservation> omopObservations, OmopObservation omopObservation) {
    if (!omopObservations.contains(omopObservation)) {
      omopObservations.add(omopObservation);
    }
  }

  /**
   * Create a new OMOP Observation with basic information extracted from FHIR Observation resource
   *
   * @param srcObservation FHIR Observation resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param labObservationConcept concept of the laboratory analysis code from the FHIR Observation
   *     resource
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @param observationSourceIdentifier identifier of the FHIR Observation resource
   * @return a new OMOP Observation with basic information extracted from FHIR Observation resource
   */
  private OmopObservation createBasisObservation(
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier) {
    String interpretation = getInterpretation(srcObservation, observationLogicId);
    var categoryCoding = getCategoryCoding(srcObservation, observationLogicId);
    //    var categoryCode = categoryCoding == null ? null : categoryCoding.getCode();

    SourceToConceptMap categoryConcept =
        findOmopConcepts.getCustomConcepts(
            categoryCoding, SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY, dbMappings);

    var newLabObservation =
        OmopObservation.builder()
            .personId(personId)
            .visitOccurrenceId(visitOccId)
            .observationTypeConceptId(categoryConcept.getTargetConceptId())
            .observationDate(effectiveDateTime.toLocalDate())
            .observationDatetime(effectiveDateTime)
            .observationSourceConceptId(observationCodeConcept.getConceptId())
            .observationConceptId(observationCodeConcept.getConceptId())
            .observationSourceValue(observationCodeConcept.getConceptCode())
            .fhirLogicalId(observationLogicId)
            .fhirIdentifier(observationSourceIdentifier)
            .build();
    if (StringUtils.isNotBlank(interpretation)) {
      var interpretationConcept =
          findOmopConcepts.getCustomConcepts(
              new Coding(null, interpretation, null),
              SOURCE_VOCABULARY_ID_LAB_INTERPRETATION,
              dbMappings);

      newLabObservation.setQualifierConceptId(interpretationConcept.getTargetConceptId());
      newLabObservation.setQualifierSourceValue(
          StringUtils.left(interpretation, MAX_SOURCE_VALUE_LENGTH));
    }
    return newLabObservation;
  }

  /**
   * Extracts value-quantity information from the FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @return value-quantity from FHIR Observation resource
   */
  private Quantity getValueQuantity(Observation srcObservation) {
    if (srcObservation.hasValueQuantity() && srcObservation.getValueQuantity() != null) {
      return srcObservation.getValueQuantity();
    }
    return null;
  }

  /**
   * Extracts value-codeable-concept information from the FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @return value-codeable-concept from FHIR Observation resource
   */
  private CodeableConcept getValueCodeableConcept(Observation srcObservation) {
    if (srcObservation.hasValueCodeableConcept()) {
      return srcObservation.getValueCodeableConcept();
    }
    return null;
  }

  /**
   * Extracts interpretation information from the FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @return interpretation from FHIR Observation resource
   */
  private String getInterpretation(Observation srcObservation, String observationLogicId) {
    if (!srcObservation.hasInterpretation() || srcObservation.getInterpretation().isEmpty()) {
      log.debug("No [Interpretation] found in [Observation]:{}.", observationLogicId);
      return null;
    }
    var interpretation =
        srcObservation.getInterpretationFirstRep().getCoding().stream()
            .filter(x -> x.getSystem().equalsIgnoreCase(fhirSystems.getInterpretation()))
            .findFirst();

    if (interpretation.isEmpty()) {
      log.debug("No [Interpretation] found in [Observation]:{}.", observationLogicId);
      return null;
    }
    return interpretation.get().getCode();
  }

  /**
   * Creates new records of the measurement table in OMOP CDM for the processed FHIR Observation
   * resource. The extracted LOINC code of the FHIR Observation resource belongs to the Measurement
   * domain in OMOP CDM.
   *
   * @param srcObservation FHIR Observation resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param labObservationConcept concept of the laboratory analysis code from the FHIR Observation
   *     resource
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @param observationSourceIdentifier identifier of the FHIR Observation resource
   * @return list of new records of the measurement table in OMOP CDM for the processed FHIR
   *     Observation resource
   */
  private void setMeasurement(
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier,
      OmopModelWrapper wrapper) {
    List<Measurement> measurements = new ArrayList<>();

    if (FHIR_RESOURCE_GECCO_OBSERVATION_IN_MEASUREMENT_DOMAIN_CODES.contains(
        observationCodeConcept.getConceptCode())) {
      setMeasurementUsingComponent(
          srcObservation,
          personId,
          visitOccId,
          observationCodeConcept,
          effectiveDateTime,
          observationLogicId,
          observationSourceIdentifier,
          wrapper);
      return;
    }

    var basisMeasurement =
        createBasisMeasurement(
            srcObservation,
            personId,
            visitOccId,
            observationCodeConcept,
            effectiveDateTime,
            observationLogicId,
            observationSourceIdentifier);
    var valueQuantity = getValueQuantity(srcObservation);
    var valueCodeableConcept = getValueCodeableConcept(srcObservation);

    if (valueQuantity == null && valueCodeableConcept == null) {
      log.debug(
          "No [ValueQuantity] or [ValueCodeableConcept] found for [Observation]:{}. Skip.",
          observationLogicId);
      //      return Collections.emptyList();
      return;
    }
    if (valueQuantity != null) {
      setValueQuantityInMeasurement(
          effectiveDateTime, measurements, basisMeasurement, valueQuantity);
    } else {
      setValueCodeableConceptInMeasurement(
          effectiveDateTime, measurements, basisMeasurement, valueCodeableConcept);
    }

    // addToList(measurements, basisMeasurement);
    //    return measurements;
    wrapper.setMeasurement(measurements);
  }

  private void setMeasurementUsingComponent(
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier,
      OmopModelWrapper wrapper) {
    var componentList = srcObservation.getComponent();
    var valueInteger = srcObservation.getValueIntegerType().getValue();
    if ((componentList == null || componentList.isEmpty()) && valueInteger == null) {
      return;
    }
    var categoryCoding = getCategoryCoding(srcObservation, observationLogicId);
    //    var categoryCode = categoryCoding == null ? null : categoryCoding.getCode();

    SourceToConceptMap categoryConcept =
        findOmopConcepts.getCustomConcepts(
            categoryCoding, SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY, dbMappings);
    if (categoryConcept == null) {
      return;
    }

    if (FHIR_RESOURCE_GECCO_OBSERVATION_SOFA_CODES.contains(
        observationCodeConcept.getConceptCode())) {
      var geccoSofaTotalMeasurement =
          createGeccoSofaTotalMeasurement(
              personId,
              visitOccId,
              observationCodeConcept,
              valueInteger,
              effectiveDateTime,
              observationLogicId,
              observationSourceIdentifier);
      geccoSofaTotalMeasurement.setMeasurementTypeConceptId(categoryConcept.getTargetConceptId());
      wrapper.getMeasurement().add(geccoSofaTotalMeasurement);
    }

    for (var component : componentList) {
      var componentCoding = getComponentCoding(component);
      if (componentCoding == null) {
        continue;
      }

      if (FHIR_RESOURCE_GECCO_OBSERVATION_BLOOD_PRESSURE_CODES.contains(
          observationCodeConcept.getConceptCode())) {

        var geccoBloodPressureMeasurement =
            createGeccoBloodPressureMeasurement(
                component,
                personId,
                visitOccId,
                observationCodeConcept,
                componentCoding,
                effectiveDateTime,
                observationLogicId,
                observationSourceIdentifier);
        if (geccoBloodPressureMeasurement != null) {
          geccoBloodPressureMeasurement.setMeasurementTypeConceptId(
              categoryConcept.getTargetConceptId());
        }

        wrapper.getMeasurement().add(geccoBloodPressureMeasurement);
      }
      if (FHIR_RESOURCE_GECCO_OBSERVATION_SOFA_CODES.contains(
          observationCodeConcept.getConceptCode())) {
        var geccoSofaMeasurement =
            createGeccoSofaMeasurement(
                component,
                personId,
                visitOccId,
                componentCoding,
                effectiveDateTime,
                observationLogicId,
                observationSourceIdentifier);
        if (geccoSofaMeasurement != null) {
          geccoSofaMeasurement.setMeasurementTypeConceptId(categoryConcept.getTargetConceptId());
        }
        wrapper.getMeasurement().add(geccoSofaMeasurement);
      }
    }
  }

  private Measurement createGeccoSofaTotalMeasurement(
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      Integer valueInteger,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier) {

    return Measurement.builder()
        .personId(personId)
        .visitOccurrenceId(visitOccId)
        .valueAsNumber(BigDecimal.valueOf(valueInteger))
        .measurementConceptId(observationCodeConcept.getConceptId())
        .measurementSourceConceptId(observationCodeConcept.getConceptId())
        .measurementSourceValue(observationCodeConcept.getConceptCode())
        .measurementDate(effectiveDateTime.toLocalDate())
        .measurementDatetime(effectiveDateTime)
        .fhirIdentifier(observationSourceIdentifier)
        .fhirLogicalId(observationLogicId)
        .build();
  }

  private Measurement createGeccoSofaMeasurement(
      ObservationComponentComponent component,
      Long personId,
      Long visitOccId,
      Coding componentCoding,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier) {
    var componentCodingConcept =
        findOmopConcepts.getCustomConcepts(
            componentCoding, SOURCE_VOCABULARY_SOFA_CATEGORY, dbMappings);
    if (componentCodingConcept == null) {
      return null;
    }
    var componentValueCodeableConcept = component.getValueCodeableConcept();
    if (componentValueCodeableConcept == null) {
      return null;
    }

    var componentValueCodeOptional =
        componentValueCodeableConcept.getCoding().stream()
            .filter(coding -> coding.getSystem().equals(fhirSystems.getGeccoSofaScore()))
            .findFirst();
    if (!componentValueCodeOptional.isPresent()) {
      return null;
    }
    var componentValueCode = componentValueCodeOptional.get().getCode();
    if (componentValueCode == null) {
      return null;
    }
    var componentValueNumber =
        BigDecimal.valueOf(Long.parseLong(componentValueCode.replaceAll("[^0-9]", "")));
    return Measurement.builder()
        .personId(personId)
        .visitOccurrenceId(visitOccId)
        .measurementDate(effectiveDateTime.toLocalDate())
        .measurementDatetime(effectiveDateTime)
        .measurementConceptId(componentCodingConcept.getTargetConceptId())
        .measurementSourceConceptId(componentCodingConcept.getTargetConceptId())
        .measurementSourceValue(componentCoding.getCode())
        .valueSourceValue(componentValueCode)
        .valueAsNumber(componentValueNumber)
        .fhirIdentifier(observationSourceIdentifier)
        .fhirLogicalId(observationLogicId)
        .build();
  }

  private Measurement createGeccoBloodPressureMeasurement(
      ObservationComponentComponent component,
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      Coding componentCoding,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier) {
    var componentCodingConcept =
        findOmopConcepts.getConcepts(
            componentCoding, effectiveDateTime.toLocalDate(), bulkload, dbMappings);
    if (componentCodingConcept == null) {
      return null;
    }
    var componentValueQuantity = component.getValueQuantity();
    if (componentValueQuantity == null) {
      return null;
    }
    var componentValueQuantityUnitCoding =
        new Coding()
            .setCode(componentValueQuantity.getCode())
            .setSystem(componentValueQuantity.getSystem());
    var componentValueQuantityUnitConcept =
        findOmopConcepts.getConcepts(
            componentValueQuantityUnitCoding,
            effectiveDateTime.toLocalDate(),
            bulkload,
            dbMappings);
    return Measurement.builder()
        .personId(personId)
        .visitOccurrenceId(visitOccId)
        .measurementDate(effectiveDateTime.toLocalDate())
        .measurementDatetime(effectiveDateTime)
        .measurementConceptId(observationCodeConcept.getConceptId())
        .measurementSourceConceptId(componentCodingConcept.getConceptId())
        .measurementSourceValue(componentCoding.getCode())
        .valueSourceValue(componentValueQuantity.getValue().toString())
        .valueAsNumber(componentValueQuantity.getValue())
        .unitSourceValue(componentValueQuantity.getUnit())
        .unitConceptId(
            componentValueQuantityUnitConcept == null
                ? null
                : componentValueQuantityUnitConcept.getConceptId())
        .fhirIdentifier(observationSourceIdentifier)
        .fhirLogicalId(observationLogicId)
        .build();
  }

  private Coding getComponentCoding(ObservationComponentComponent component) {
    var codeableConcept = component.getCode();
    if (codeableConcept == null) {
      return null;
    }
    var codingList = codeableConcept.getCoding();
    if (codingList.isEmpty()) {
      return null;
    }
    var codingCodeOptional =
        codingList.stream()
            .filter(coding -> fhirSystems.getGeccoComponents().contains(coding.getSystem()))
            .findFirst();
    if (!codingCodeOptional.isPresent()) {
      return null;
    }
    return codingCodeOptional.get();
  }

  /**
   * Set value as code for OMOP MEASUREMENT entry
   *
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param measurements a list of OMOP MEASUREMENT entries
   * @param basisMeasurement the basic MEASUREMENT entry
   * @param valueCodeableConcept value from Observation FHIR resource as code
   */
  private void setValueCodeableConceptInMeasurement(
      LocalDateTime effectiveDateTime,
      List<Measurement> measurements,
      Measurement basisMeasurement,
      CodeableConcept valueCodeableConcept) {
    if (valueCodeableConcept.isEmpty()) {
      return;
    }
    if (valueCodeableConcept.getCoding().isEmpty()) {

      var valueConcept =
          findOmopConcepts.getCustomConcepts(
              new Coding(null, valueCodeableConcept.getText(), null),
              SOURCE_VOCABULARY_ID_LAB_RESULT,
              dbMappings);

      basisMeasurement.setValueAsConceptId(valueConcept.getTargetConceptId());
      basisMeasurement.setValueSourceValue(valueConcept.getSourceCode());

    } else {
      for (var coding : valueCodeableConcept.getCoding()) {
        var codingCode = coding.getCode();
        if (codingCode == null || Strings.isNullOrEmpty(codingCode)) {
          continue;
        }
        var codingSystemUrl = coding.getSystem();
        if (codingSystemUrl.equals(fhirSystems.getGeccoFrailtyScore())) {
          var valueCodingConcept =
              findOmopConcepts.getCustomConcepts(
                  coding, SOURCE_VOCABULARY_FRAILTY_SCORE, dbMappings);
          if (valueCodingConcept != null) {
            basisMeasurement.setValueAsConceptId(valueCodingConcept.getTargetConceptId());
            basisMeasurement.setValueSourceValue(codingCode);
          }
        } else {
          var valueCodingConcept =
              findOmopConcepts.getConcepts(
                  coding, effectiveDateTime.toLocalDate(), bulkload, dbMappings);
          if (valueCodingConcept == null) {
            continue;
          }
          basisMeasurement.setValueAsConceptId(valueCodingConcept.getConceptId());
          basisMeasurement.setValueSourceValue(valueCodingConcept.getConceptCode());
        }
        addToList(measurements, basisMeasurement);
      }
    }
  }

  /**
   * Set value as quantity for OMOP MEASUREMENT entry
   *
   * @param effectiveDateTimedate time of the FHIR Observation resource
   * @param basisMeasurement the basic MEASUREMENT entry
   * @param valueQuantity value from Observation FHIR resource as quantity
   */
  private void setValueQuantityInMeasurement(
      LocalDateTime effectiveDateTime,
      List<Measurement> measurements,
      Measurement basisMeasurement,
      Quantity valueQuantity) {
    basisMeasurement.setValueAsNumber(valueQuantity.getValue());
    basisMeasurement.setValueSourceValue(valueQuantity.getValue().toString());

    var quantityUnitCodingFormat =
        new Coding().setCode(valueQuantity.getCode()).setSystem(valueQuantity.getSystem());

    var valueQuantityUnitConcept =
        findOmopConcepts.getConcepts(
            quantityUnitCodingFormat, effectiveDateTime.toLocalDate(), bulkload, dbMappings);
    if (valueQuantityUnitConcept != null) {
      basisMeasurement.setUnitConceptId(valueQuantityUnitConcept.getConceptId());
      basisMeasurement.setUnitSourceValue(
          valueQuantity.getUnit() == null
              ? valueQuantityUnitConcept.getConceptCode()
              : valueQuantity.getUnit());
      addToList(measurements, basisMeasurement);
    }
  }

  /**
   * Add new OMOP MEASUREMENT entry to a list of MEASUREMENT entries without duplicates
   *
   * @param omopMeasurements a list of MEASUREMENT entries
   * @param omopMeasurement new OMOP MEASUREMENT entry
   */
  private void addToList(List<Measurement> omopMeasurements, Measurement omopMeasurement) {
    if (!omopMeasurements.contains(omopMeasurement)) {
      omopMeasurements.add(omopMeasurement);
    }
  }
  /**
   * Create a new Measurement with basic information extracted from FHIR Observation resource
   *
   * @param srcObservation FHIR Observation resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param labObservationConcept concept of the laboratory analysis code from the FHIR Observation
   *     resource
   * @param effectiveDateTime date time of the FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @param observationSourceIdentifier identifier of the FHIR Observation resource
   * @return a new Measurement with basic information extracted from FHIR Observation resource
   */
  private Measurement createBasisMeasurement(
      Observation srcObservation,
      Long personId,
      Long visitOccId,
      Concept observationCodeConcept,
      LocalDateTime effectiveDateTime,
      String observationLogicId,
      String observationSourceIdentifier) {
    var categoryCoding = getCategoryCoding(srcObservation, observationLogicId);
    //    var categoryCode = categoryCoding == null ? null : categoryCoding.getCode();

    SourceToConceptMap categoryConcept =
        findOmopConcepts.getCustomConcepts(
            categoryCoding, SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY, dbMappings);

    String interpretation = getInterpretation(srcObservation, observationLogicId);

    var newLabMeasurement =
        Measurement.builder()
            .personId(personId)
            .measurementDate(effectiveDateTime.toLocalDate())
            .measurementDatetime(effectiveDateTime)
            .measurementTypeConceptId(categoryConcept.getTargetConceptId())
            .visitOccurrenceId(visitOccId)
            .measurementConceptId(observationCodeConcept.getConceptId())
            .measurementSourceConceptId(observationCodeConcept.getConceptId())
            .measurementSourceValue(observationCodeConcept.getConceptCode())
            .fhirLogicalId(observationLogicId)
            .fhirIdentifier(observationSourceIdentifier)
            .build();

    if (!Strings.isNullOrEmpty(interpretation)) {
      var interpretationConcept =
          findOmopConcepts.getCustomConcepts(
              new Coding(null, interpretation, null),
              SOURCE_VOCABULARY_ID_LAB_INTERPRETATION,
              dbMappings);
      newLabMeasurement.setOperatorConceptId(interpretationConcept.getTargetConceptId());
    }
    setReferenceRange(srcObservation, newLabMeasurement, observationLogicId);
    return newLabMeasurement;
  }

  /**
   * Sets the extracted reference range information from FHIR Observation resource to the new
   * measurement record.
   *
   * @param srcObservation FHIR Observation resource
   * @param newLabMeasurement record of the measurement table in OMOP CDM for the processed FHIR
   *     Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   */
  private void setReferenceRange(
      Observation srcObservation, Measurement newLabMeasurement, String observationLogicId) {
    var referenceRange = getReferenceRange(srcObservation);

    if (referenceRange == null) {
      log.debug("No [Reference Range] found in [Observation]:{}.", observationLogicId);
      return;
    }

    if (referenceRange.hasHigh() && referenceRange.getHigh() != null) {
      newLabMeasurement.setRangeHigh(referenceRange.getHigh().getValue());
    } else {
      log.debug("Missing [high range] for [Observation]:{}.", observationLogicId);
    }

    if (referenceRange.hasLow() && referenceRange.getLow() != null) {
      newLabMeasurement.setRangeLow(referenceRange.getLow().getValue());
    } else {
      log.debug("Missing [low range] for [Observation]:{}.", observationLogicId);
    }
  }

  /**
   * Extracts reference range information from the FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @return reference range from FHIR Observation resource
   */
  private ObservationReferenceRangeComponent getReferenceRange(Observation srcObservation) {
    if (srcObservation.hasReferenceRange()
        && !srcObservation.getReferenceRangeFirstRep().isEmpty()) {
      return srcObservation.getReferenceRangeFirstRep();
    }
    return null;
  }

  /**
   * Extracts category information from the FHIR Observation resource.
   *
   * @param srcObservation FHIR Observation resource
   * @param observationLogicId logical id of the FHIR Observation resource
   * @return category from FHIR Observation resource
   */
  private Coding getCategoryCoding(Observation srcObservation, String observationLogicId) {
    if (!srcObservation.hasCategory() || srcObservation.getCategory().isEmpty()) {
      log.warn("No [Category] found for {}. Invalid resource. Please Check.", observationLogicId);
      return null;
    }
    var categories = srcObservation.getCategory();
    for (var category : categories) {
      var categoryCode =
          category.getCoding().stream()
              //              .filter(cat ->
              // cat.getSystem().equals(fhirSystems.getLabObservationCategory()))
              .filter(cat -> fhirSystems.getLabObservationCategory().contains(cat.getSystem()))
              .findFirst();
      if (!categoryCode.isEmpty()) {
        return categoryCode.get();
      }
    }
    return null;
  }

  /**
   * Delete FHIR Observation resources from OMOP CDM tables using fhir_logical_id and
   * fhir_identifier
   *
   * @param observationLogicId logical id of the FHIR Observation resource
   * @param observationSourceIdentifier identifier of the FHIR Observation resource
   */
  private void deleteExistingLabObservations(
      String observationLogicId, String observationSourceIdentifier) {
    if (!Strings.isNullOrEmpty(observationLogicId)) {
      observationService.deleteExistingLabObservationByFhirLogicalId(observationLogicId);
    } else {
      observationService.deleteExistingLabObservationByFhirIdentifier(observationSourceIdentifier);
    }
  }
}
