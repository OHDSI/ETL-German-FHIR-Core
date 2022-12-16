package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_FOR_RESUSCITATION;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NOT_FOR_RESUSCITATION;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_RESUSCITATION_STATUS;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONSENT_ACCEPTABLE_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.SNOMED_FOR_RESUSCITATION;
import static org.miracum.etl.fhirtoomop.Constants.SNOMED_NOT_FOR_RESUSCITATION;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Consent;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.repository.service.ConsentMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The ConsentMapper class describes the business logic of transforming a FHIR Consent resource to
 * OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class ConsentMapper implements FhirMapper<Consent> {

  private final Boolean bulkload;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessConsent");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessConsent");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessConsent");
  private static final Counter noCodeCounter = MapperMetrics.setNoCodeCounter("stepProcessConsent");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessConsent");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessConsent");

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ConsentMapperServiceImpl consentService;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;

  /**
   * Constructor for objects of the class ConsentMapper.
   *
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public ConsentMapper(Boolean bulkload) {
    this.bulkload = bulkload;
  }

  /**
   * Maps a FHIR Consent resource to observation table in OMOP CDM.
   *
   * @param srcConsent FHIR Consent resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Consent resource
   */
  @Override
  public OmopModelWrapper map(Consent srcConsent, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var consentLogicId = fhirReferenceUtils.extractId(srcConsent);

    var consentSourceIdentifier = fhirReferenceUtils.extractResourceFirstIdentifier(srcConsent);
    if (Strings.isNullOrEmpty(consentLogicId) && Strings.isNullOrEmpty(consentSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Consent] resource is invalid. Skip resource");

      noFhirReferenceCounter.increment();
      return null;
    }

    if (bulkload.equals(Boolean.FALSE)) {

      deleteExistingObservation(consentLogicId, consentSourceIdentifier);

      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", consentLogicId);
        return null;
      }
    }

    var statusElement = srcConsent.getStatusElement();
    var statusValue = checkDataAbsentReason.getValue(statusElement);
    if (Strings.isNullOrEmpty(statusValue)
        || !FHIR_RESOURCE_CONSENT_ACCEPTABLE_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status] of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          consentLogicId);
      return null;
    }

    var consentCategoryCoding = getResuscitateStatusCategoryCode(srcConsent);
    if (consentCategoryCoding == null) {
      log.warn("No Category [dnr] found in [Consent]:{}. Skip resource", consentLogicId);
      return null;
    }

    var personId = getPersonId(srcConsent, consentLogicId);
    if (personId == null) {
      log.warn("No matching [Person] found for {}. Skip resource", consentLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    //    var consentScopeCodings = getConsentScopeCoding(srcConsent);
    //    if (consentScopeCodings.isEmpty()) {
    //      log.warn("No Scope found in [Consent]:{}. Skip resource", consentLogicId);
    //      noCodeCounter.increment();
    //      return null;
    //    }

    var consentOnset = getConsentOnset(srcConsent);
    if (consentOnset.getStartDateTime() == null) {
      log.warn("Unable to determine [dateTime] for {}. Skip resource", consentLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var provisionCode = getConsentProvision(srcConsent);
    if (provisionCode == null) {
      log.warn("No provision code found in [Consent]:{}. Skip resource", consentLogicId);
      noCodeCounter.increment();
      return null;
    }

    var newResuscitationStatus =
        createOmopObservation(
            personId, consentOnset, provisionCode, consentLogicId, consentSourceIdentifier);
    if (newResuscitationStatus == null) {
      return null;
    }
    wrapper.getObservation().add(newResuscitationStatus);

    return wrapper;
  }

  private String getConsentProvision(Consent srcConsent) {
    var provision = checkDataAbsentReason.getValue(srcConsent.getProvision());
    if (provision == null) {
      return null;
    }
    var provisionCodeableConcept = checkDataAbsentReason.getValue(provision.getCodeFirstRep());
    if (provisionCodeableConcept == null) {
      return null;
    }
    var provisionCodingList = provisionCodeableConcept.getCoding();
    if (provisionCodingList.isEmpty()) {
      return null;
    }
    // In the Resuscitation-Status FHIR profile, should exist only one value in provision-Element
    var coding = checkDataAbsentReason.getValue(provisionCodingList.get(0));
    if (coding == null) {
      return null;
    }
    var provisionCode = checkDataAbsentReason.getValue(coding.getCodeElement());
    if (provisionCode == null || provisionCode.equalsIgnoreCase("unknown")) {
      return null;
    }
    return provisionCode;
  }

  private OmopObservation createOmopObservation(
      Long personId,
      ResourceOnset consentOnset,
      String provisionCode,
      String consentLogicId,
      String consentSourceIdentifier) {
    Integer provisionConceptId;
    if (provisionCode.equals(SNOMED_FOR_RESUSCITATION)) {
      provisionConceptId = CONCEPT_FOR_RESUSCITATION;
    } else if (provisionCode.equals(SNOMED_NOT_FOR_RESUSCITATION)) {
      provisionConceptId = CONCEPT_NOT_FOR_RESUSCITATION;
    } else {
      log.warn(
          "The [provision code] {} of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          provisionCode,
          consentLogicId);
      return null;
    }

    return OmopObservation.builder()
        .personId(personId)
        .observationConceptId(CONCEPT_RESUSCITATION_STATUS)
        .observationTypeConceptId(CONCEPT_EHR)
        .observationDate(consentOnset.getStartDateTime().toLocalDate())
        .observationDatetime(consentOnset.getStartDateTime())
        .observationSourceValue(provisionCode)
        .observationSourceConceptId(provisionConceptId)
        .valueAsString(provisionCode)
        .valueAsConceptId(provisionConceptId)
        .fhirIdentifier(consentSourceIdentifier)
        .fhirLogicalId(consentLogicId)
        .build();
  }

  /**
   * Delete FHIR Consent resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param consentLogicId logical id of the FHIR Consent resource
   * @param consentSourceIdentifier identifier of the FHIR Consent resource
   */
  private void deleteExistingObservation(String consentLogicId, String consentSourceIdentifier) {
    if (!Strings.isNullOrEmpty(consentLogicId)) {
      consentService.deleteExistingConsentObservationByFhirLogicalId(consentLogicId);
    } else {
      consentService.deleteExistingConsentObservationByFhirIdentifier(consentSourceIdentifier);
    }
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR Consent
   * resource.
   *
   * @param srcConsent FHIR Consent resource
   * @param consentLogicId logical id of the FHIR Consent resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Consent srcConsent, String consentLogicId) {
    var patientReferenceIdentifier = fhirReferenceUtils.getSubjectReferenceIdentifier(srcConsent);
    var patientReferenceLogicalId = fhirReferenceUtils.getSubjectReferenceLogicalId(srcConsent);
    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, consentLogicId);
  }

  /**
   * Extracts date time information from the FHIR Consent resource.
   *
   * @param srcConsent FHIR Consent resource
   * @return date time of the FHIR Consent resource
   */
  private ResourceOnset getConsentOnset(Consent srcConsent) {
    var resourceOnset = new ResourceOnset();

    if (!srcConsent.hasDateTimeElement()) {
      return resourceOnset;
    }
    var dateTimeElement = srcConsent.getDateTimeElement();
    var dateTimeValue = checkDataAbsentReason.getValue(dateTimeElement);
    if (dateTimeValue == null) {
      return resourceOnset;
    }
    resourceOnset.setStartDateTime(dateTimeValue);
    return resourceOnset;
  }

  private String getResuscitateStatusCategoryCode(Consent srcConsent) {
    var consentCategoryList = srcConsent.getCategory();

    if (consentCategoryList.isEmpty()) {
      return null;
    }

    for (var category : consentCategoryList) {
      var consentCategoryCodings = category.getCoding();
      if (consentCategoryCodings.isEmpty()) {
        continue;
      }
      var categoryCode =
          consentCategoryCodings.stream()
              .filter(
                  cat ->
                      cat.getSystem()
                              .equalsIgnoreCase(
                                  "http://terminology.hl7.org/CodeSystem/consentcategorycodes")
                          && checkDataAbsentReason
                              .getValue(cat.getCodeElement())
                              .equalsIgnoreCase("dnr"))
              .findFirst();
      if (categoryCode.isPresent()) {
        return categoryCode.get().getCode();
      }
      //      for (var coding : consentCategoryCodings) {
      //        var code = checkDataAbsentReason.getValue(coding.getCodeElement());
      //        log.info("aksdljaf {}", code);
      //        if (code == null) {
      //          continue;
      //        }
      //        return code;
      //      }
    }

    return null;
  }

  /**
   * Check if the used consent code exists in FHIR Consent resource
   *
   * @param consentCoding consent codings from the FHIR Consent resource
   * @return a boolean value
   */
  private boolean checkIfCodeExist(Coding consentCoding) {
    var codeElement = consentCoding.getCodeElement();
    if (codeElement.isEmpty()) {
      return false;
    }
    var code = checkDataAbsentReason.getValue(codeElement);
    if (Strings.isNullOrEmpty(code)) {
      return false;
    }

    return true;
  }
}
