package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR_MEDICATION_LIST;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_ADMINISTRATION_ACCEPTABLE_STATUS_LIST;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_DRUG;
import static org.miracum.etl.fhirtoomop.Constants.OMOP_DOMAIN_OBSERVATION;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.MedicationAdministration;
import org.hl7.fhir.r4.model.MedicationAdministration.MedicationAdministrationDosageComponent;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.ResourceType;
import org.hl7.fhir.r4.model.StringType;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.AtcStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.DrugExposure;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.repository.service.DrugExposureMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.EncounterDepartmentCaseMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.MedicationAdministrationMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * The MedicationAdministrationMapper class describes the business logic of transforming a FHIR
 * MedicationAdministration resource to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class MedicationAdministrationMapper implements FhirMapper<MedicationAdministration> {
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final IFhirPath fhirPath;
  private final ResourceFhirReferenceUtils referenceUtils;
  private final Boolean bulkload;
  private final DbMappings dbMappings;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired MedicationAdministrationMapperServiceImpl medicationAdministrationService;
  @Autowired DrugExposureMapperServiceImpl drugExposureMapperService;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;
  @Autowired
  EncounterDepartmentCaseMapperServiceImpl departmentCaseMapperService;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessMedicationAdministrations");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessMedicationAdministrations");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessMedicationAdministrations");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessMedicationAdministrations");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessMedicationAdministrations");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessMedicationAdministrations");
  private static final Counter statusNotAcceptableCounter =
          MapperMetrics.setStatusNotAcceptableCounter("stepProcessMedicationAdministrations");
  private static final Counter noMatchingEncounterCounter =
          MapperMetrics.setNoMatchingEncounterCount("stepProcessMedicationAdministrations");
  private static final Counter invalidDoesCounter =
          MapperMetrics.setInvalidDoseCounter("stepProcessMedicationAdministrations");
  private static final Counter invalidDosageCounter =
          MapperMetrics.setInvalidDosageCounter("stepProcessMedicationAdministrations");
  private static final Counter invalidRouteValueCounter =
          MapperMetrics.setInvalidRouteValueCounter("stepProcessMedicationAdministrations");
  /**
   * Constructor for objects of the class MedicationAdministrationMapper.
   *
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public MedicationAdministrationMapper(
      IFhirPath fhirPath,
      ResourceFhirReferenceUtils referenceUtils,
      Boolean bulkload,
      DbMappings dbMappings) {

    this.fhirPath = fhirPath;
    this.referenceUtils = referenceUtils;
    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR MedicationAdministration resource to drug_exposure table in OMOP CDM.
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR
   *     MedicationAdministration resource
   */
  @Override
  public OmopModelWrapper map(
      MedicationAdministration srcMedicationAdministration, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var medicationAdministrationLogicId = fhirReferenceUtils.extractId(srcMedicationAdministration);
    var medicationAdministrationSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcMedicationAdministration);

    if (Strings.isNullOrEmpty(medicationAdministrationLogicId)
        && Strings.isNullOrEmpty(medicationAdministrationSourceIdentifier)) {
      log.warn(
          "No [Identifier] or [Id] found. [MedicationAdministration] resource is invalid. Skip resource.");
      noFhirReferenceCounter.increment();
      return null;
    }
    String medicationAdministrationId = "";
    if (!Strings.isNullOrEmpty(medicationAdministrationLogicId)) {
      medicationAdministrationId = srcMedicationAdministration.getId();
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingMedicationAdministrationEntry(
          medicationAdministrationLogicId, medicationAdministrationSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info(
            "Found a deleted [MedicationAdministration] resource {}. Deleting from OMOP DB.",
            medicationAdministrationId);
        return null;
      }
    }

    var statusElement = srcMedicationAdministration.getStatusElement();
    var statusValue = checkDataAbsentReason.getValue(statusElement);
    if (Strings.isNullOrEmpty(statusValue)
        || !FHIR_RESOURCE_MEDICATION_ADMINISTRATION_ACCEPTABLE_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status]: {} of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          statusValue,
          medicationAdministrationId);
      statusNotAcceptableCounter.increment();
      return null;
    }

    var personId =
        getPersonId(
            srcMedicationAdministration,
            medicationAdministrationLogicId,
            medicationAdministrationId);
    if (personId == null) {
      log.warn(
          "No matching [Person] found for [MedicationAdministration]: {}. Skip resource",
          medicationAdministrationId);

      noPersonIdCounter.increment();
      return null;
    }

    var onset = getMedicationAdministrationOnset(srcMedicationAdministration);
    if (onset.getStartDateTime() == null) {
      log.warn(
          "Unable to determine the [datetime] for [MedicationAdministration]: {}. Skip resource",
          medicationAdministrationId);

      noStartDateCounter.increment();
      return null;
    }

    var medicationReferenceLogicalId = getMedicationReferenceLogicalId(srcMedicationAdministration);
    var medicationReferenceIdentifier =
        getMedicationReferenceIdentifier(srcMedicationAdministration);

    var atcCoding =
        getMedCoding(
            medicationReferenceLogicalId,
            medicationReferenceIdentifier,
            srcMedicationAdministration);
    if (atcCoding == null) {
      log.warn(
          "Unable to determine the [medication code] for [MedicationAdministration]: {}. Skip resource",
          medicationAdministrationId);
      noCodeCounter.increment();
      return null;
    }

    var visitOccId =
        getVisitOccId(srcMedicationAdministration, personId, medicationAdministrationId);
    var medDosage = getDosage(srcMedicationAdministration, medicationAdministrationId);

    createMedicationMapping(
        wrapper,
        medDosage,
        onset,
        personId,
        visitOccId,
        atcCoding,
        medicationAdministrationLogicId,
        medicationAdministrationSourceIdentifier,
        medicationAdministrationId);

    return wrapper;
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR
   * MedicationAdministration resource.
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(
      MedicationAdministration srcMedicationAdministration,
      String medicationAdministrationLogicId,
      String medicationAdministrationId) {
    var patientReferenceIdentifier =
        referenceUtils.getSubjectReferenceIdentifier(srcMedicationAdministration);
    var patientReferenceLogicalId =
        referenceUtils.getSubjectReferenceLogicalId(srcMedicationAdministration);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier,
        patientReferenceLogicalId,
        medicationAdministrationLogicId,
        medicationAdministrationId);
  }

  /**
   * Extracts date time information from the FHIR MedicationAdministration resource.
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @return start date time and end date time of the FHIR MedicationAdministration resource
   */
  private ResourceOnset getMedicationAdministrationOnset(
      MedicationAdministration srcMedicationAdministration) {
    var resourceOnset = new ResourceOnset();

    var effective = checkDataAbsentReason.getValue(srcMedicationAdministration.getEffective());
    if (effective == null) {
      return resourceOnset;
    }

    if (srcMedicationAdministration.hasEffectiveDateTimeType()) {
      var effectiveDateTime =
          checkDataAbsentReason.getValue(srcMedicationAdministration.getEffectiveDateTimeType());
      if (effectiveDateTime != null) {
        resourceOnset.setStartDateTime(effectiveDateTime);
        return resourceOnset;
      }
    }

    if (srcMedicationAdministration.hasEffectivePeriod()) {

      var effectivePeriod =
          checkDataAbsentReason.getValue(srcMedicationAdministration.getEffectivePeriod());
      if (effectivePeriod != null) {
        if (effectivePeriod.getStart() != null) {
          resourceOnset.setStartDateTime(
              new Timestamp(effectivePeriod.getStart().getTime()).toLocalDateTime());
        }
        if (effectivePeriod.getEnd() != null) {
          resourceOnset.setEndDateTime(
              new Timestamp(effectivePeriod.getEnd().getTime()).toLocalDateTime());
        }
      }
    }
    var fhirLogicalId = fhirReferenceUtils.extractId(ResourceType.Encounter.name(), srcMedicationAdministration.getContext().getReferenceElement().getIdPart());
    var visitDetail = departmentCaseMapperService.getVisitStartDateTimeByFhirLogicId(fhirLogicalId);
    if(visitDetail != null){
      resourceOnset.setStartDateTime(visitDetail.getVisitDetailStartDatetime());
      resourceOnset.setEndDateTime(visitDetail.getVisitDetailEndDatetime());
    }
    return resourceOnset;
  }

  /**
   * Extracts the logical id of the referenced FHIR MedicationAdministration resource for the
   * processed FHIR MedicationAdministration resource.
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @return logical id of the referenced FHIR MedicationAdministration resource
   */
  private String getMedicationReferenceLogicalId(
      MedicationAdministration srcMedicationAdministration) {
    // ID of the corresponding medication resource from medication reference
    if (srcMedicationAdministration.hasMedicationReference()
        && srcMedicationAdministration.getMedicationReference().hasReference()) {
      return "med-"
          + srcMedicationAdministration.getMedicationReference().getReferenceElement().getIdPart();
    }

    return null;
  }

  /**
   * Extracts the identifier of the referenced FHIR MedicationAdministration resource for the
   * processed FHIR MedicationAdministration resource.
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @return identifier of the referenced FHIR MedicationAdministration resource
   */
  private String getMedicationReferenceIdentifier(
      MedicationAdministration srcMedicationAdministration) {

    if (!srcMedicationAdministration.hasMedicationReference()
        || srcMedicationAdministration.getMedicationReference() == null) {
      return null;
    }

    if (srcMedicationAdministration.getMedicationReference().hasIdentifier()
        && !srcMedicationAdministration.getMedicationReference().getIdentifier().isEmpty()
        && srcMedicationAdministration.getMedicationReference().getIdentifier() != null
        && srcMedicationAdministration.getMedicationReference().getIdentifier().hasValue()
        && srcMedicationAdministration.getMedicationReference().getIdentifier().getValue()
            != null) {
      return "med-"
          + srcMedicationAdministration.getMedicationReference().getIdentifier().getValue();
    }

    return null;
  }

  /**
   * Returns the visit_occurrence_id of the referenced FHIR Encounter resource for the processed
   * FHIR MedicationAdministration resource.
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(
      MedicationAdministration srcMedicationAdministration,
      Long personId,
      String medicationAdministrationId) {
    var encounterReferenceIdentifier = getVisitReferenceIdentifier(srcMedicationAdministration);
    var encounterReferenceLogicalId = getVisitReferenceLogicalId(srcMedicationAdministration);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            medicationAdministrationId);
    if (visitOccId == null) {
      log.debug(
          "No matching [Encounter] found for [MedicationAdministration]: {}.",
          medicationAdministrationId);
      noMatchingEncounterCounter.increment();
    }
    return visitOccId;
  }

  /**
   * Extracts the logical id of the referenced FHIR Encounter resource (supply case/administrative
   * case) for the processed FHIR MedicationAdministration resource.
   *
   * @param fromMedicationAdministration FHIR MedicationAdministration resource
   * @return logical id of the referenced FHIR Encounter resource
   */
  private String getVisitReferenceLogicalId(MedicationAdministration fromMedicationAdministration) {
    var referencePath = "context.reference";
    var logicalId =
        fhirPath.evaluateFirst(fromMedicationAdministration, referencePath, StringType.class);

    if (logicalId.isPresent()) {
      var reference = new Reference(logicalId.get().getValue());
      return "enc-" + reference.getReferenceElement().getIdPart();
    }

    return null;
  }

  /**
   * Extracts the identifier of the referenced FHIR Encounter resource (supply case/administrative
   * case) for the processed FHIR MedicationAdministration resource.
   *
   * @param fromMedicationAdministration FHIR MedicationAdministration resource
   * @return identifier of the referenced FHIR Encounter resource
   */
  private String getVisitReferenceIdentifier(
      MedicationAdministration fromMedicationAdministration) {
    var identifierByTypePath =
        String.format(
            "context.identifier.where(type.coding.system='%s' and type.coding.code='VN').value",
            fhirSystems.getIdentifierType());
    var identifier =
        fhirPath.evaluateFirst(
            fromMedicationAdministration, identifierByTypePath, StringType.class);

    if (identifier.isPresent()) {
      return "enc-" + identifier.get().getValue();
    }

    return null;
  }

  public void createMedicationMapping(
      OmopModelWrapper wrapper,
      MedicationAdministrationDosageComponent medDosage,
      ResourceOnset onset,
      Long personId,
      Long visitOccId,
      Coding atcCoding,
      String medicationAdministrationLogicId,
      String medicationAdministrationSourceIdentifier,
      String medicationAdministrationId) {

    var atcStandardMapPairList =
        getValidAtcCodes(
            atcCoding, onset.getStartDateTime().toLocalDate(), medicationAdministrationId);

    if (atcStandardMapPairList.isEmpty()) {
      return;
    }
    for (var singlePair : atcStandardMapPairList) {
      medicationProcessor(
          singlePair,
          wrapper,
          medDosage,
          onset,
          personId,
          visitOccId,
          medicationAdministrationLogicId,
          medicationAdministrationSourceIdentifier,
          medicationAdministrationId);
    }
  }

  /**
   * Extract valid pairs of Atc code and its OMOP concept_id and domain information as a list
   *
   * @param atcCoding
   * @param startDate the start date of the MedicationAdministration
   * @return a list of valid pairs of ATC code and its OMOP concept_id and domain information
   */
  private List<Pair<String, List<AtcStandardDomainLookup>>> getValidAtcCodes(
      Coding atcCoding, LocalDate startDate, String medicationAdministrationId) {
    if (atcCoding == null) {
      return Collections.emptyList();
    }

    List<Pair<String, List<AtcStandardDomainLookup>>> validAtcStandardConceptMaps =
        new ArrayList<>();
    List<AtcStandardDomainLookup> atcStandardMap =
        findOmopConcepts.getAtcStandardConcepts(
            atcCoding, startDate, bulkload, dbMappings, medicationAdministrationId);
    if (atcStandardMap.isEmpty()) {
      return Collections.emptyList();
    }

    validAtcStandardConceptMaps.add(Pair.of(atcCoding.getCode(), atcStandardMap));

    return validAtcStandardConceptMaps;
  }

  /**
   * Processes information from FHIR MedicationAdministration resource and transforms them into
   * records OMOP CDM tables.
   *
   * @param atcStandardPair one pair of ATC code and its OMOP standard concept_id and domain
   *     information
   * @param wrapper the OMOP model wrapper
   * @param medDosage dosage information form the FHIR MedicationAdministration resource
   * @param onset start date time and end date time of the FHIR MedicationAdministration resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visiOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @param medicationAdministrationSourceIdentifier identifier of the FHIR MedicationAdministration
   *     resource
   */
  private void medicationProcessor(
      @Nullable Pair<String, List<AtcStandardDomainLookup>> atcStandardPair,
      OmopModelWrapper wrapper,
      MedicationAdministrationDosageComponent medDosage,
      ResourceOnset onset,
      Long personId,
      Long visitOccId,
      String medicationAdministrationLogicId,
      String medicationAdministrationSourceIdentifier,
      String medicationAdministrationId) {

    if (atcStandardPair == null) {
      return;
    }

    var atcCode = atcStandardPair.getLeft();
    var atcStandardMaps = atcStandardPair.getRight();

    for (var atcStandardMap : atcStandardMaps) {
      setMedication(
          wrapper,
          medDosage,
          onset,
          personId,
          visitOccId,
          medicationAdministrationLogicId,
          medicationAdministrationSourceIdentifier,
          atcCode,
          atcStandardMap.getStandardConceptId(),
          atcStandardMap.getSourceConceptId(),
          atcStandardMap.getStandardDomainId(),
          medicationAdministrationId);
    }
  }

  /** Write MedicationAdministration information into correct OMOP tables based on their domains. */
  private void setMedication(
      OmopModelWrapper wrapper,
      MedicationAdministrationDosageComponent medDosage,
      ResourceOnset onset,
      Long personId,
      Long visitOccId,
      String medicationAdministrationLogicId,
      String medicationAdministrationSourceIdentifier,
      String medicationCode,
      Integer medicationConceptId,
      Integer medicationSourceConceptId,
      String domain,
      String medicationAdministrationId) {
    switch (domain) {
      case OMOP_DOMAIN_DRUG:
        var drug =
            setUpDrugExposure(
                onset,
                medDosage,
                medicationConceptId,
                medicationSourceConceptId,
                medicationCode,
                personId,
                visitOccId,
                medicationAdministrationLogicId,
                medicationAdministrationSourceIdentifier,
                medicationAdministrationId);

        wrapper.getDrugExposure().add(drug);

        break;
      case OMOP_DOMAIN_OBSERVATION:
        var observation =
            setUpObservation(
                onset,
                medDosage,
                medicationConceptId,
                medicationSourceConceptId,
                medicationCode,
                personId,
                visitOccId,
                medicationAdministrationLogicId,
                medicationAdministrationSourceIdentifier,
                medicationAdministrationId);

        wrapper.getObservation().add(observation);

        break;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported domain %s", domain));
    }
  }

  /**
   * Creates a new record of the drug_exposure table in OMOP CDM for the processed FHIR
   * MedicationAdministration resource.
   *
   * @param onset start date time and end date time of the FHIR MedicationAdministration resource
   * @param medDosage Dosage information from MedicationAdministration resource
   * @param medicationConceptId concept id of the standard concept
   * @param medicationSourceConceptId concept id of the ATC code
   * @param medicationCode ATC code
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @param medicationAdministrationSourceIdentifier identifier of the FHIR MedicationAdministration
   *     resource
   * @return new record of the drug_exposure table in OMOP CDM for the processed FHIR
   *     MedicationAdministration resource
   */
  private DrugExposure setUpDrugExposure(
      ResourceOnset onset,
      MedicationAdministrationDosageComponent medDosage,
      Integer medicationConceptId,
      Integer medicationSourceConceptId,
      String medicationCode,
      Long personId,
      Long visitOccId,
      String medicationAdministrationLogicId,
      String medicationAdministrationSourceIdentifier,
      String medicationAdministrationId) {

    var startDateTime = onset.getStartDateTime();
    var endDateTime = onset.getEndDateTime();

    var dose = getDose(medDosage, medicationAdministrationId);
    var route = getRouteCoding(medDosage, medicationAdministrationId);

    var drugExposure =
        DrugExposure.builder()
            .drugExposureStartDate(startDateTime.toLocalDate())
            .drugExposureStartDatetime(startDateTime)
            .drugExposureEndDatetime(endDateTime)
            .drugExposureEndDate(
                endDateTime == null ? startDateTime.toLocalDate() : endDateTime.toLocalDate())
            .personId(personId)
            .drugSourceConceptId(medicationSourceConceptId)
            .drugConceptId(medicationConceptId)
            .visitOccurrenceId(visitOccId)
            .drugTypeConceptId(CONCEPT_EHR_MEDICATION_LIST)
            .drugSourceValue(medicationCode)
            .fhirLogicalId(medicationAdministrationLogicId)
            .fhirIdentifier(medicationAdministrationSourceIdentifier)
            .build();

    if (dose != null) {
      drugExposure.setDoseUnitSourceValue(dose.getUnit());
      drugExposure.setQuantity(dose.getValue());
    }

    if (route != null) {
      var routeConcept =
          findOmopConcepts.getConcepts(
              route, startDateTime.toLocalDate(), bulkload, dbMappings, medicationAdministrationId);
      drugExposure.setRouteSourceValue(routeConcept.getConceptCode());
      drugExposure.setRouteConceptId(
          routeConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
              ? null
              : routeConcept.getConceptId());
    }

    return drugExposure;
  }

  private OmopObservation setUpObservation(
      ResourceOnset onset,
      MedicationAdministrationDosageComponent medDosage,
      Integer medicationConceptId,
      Integer medicationSourceConceptId,
      String medicationCode,
      Long personId,
      Long visitOccId,
      String medicationAdministrationLogicId,
      String medicationAdministrationSourceIdentifier,
      String medicationAdministrationId) {

    var startDateTime = onset.getStartDateTime();

    var dose = getDose(medDosage, medicationAdministrationId);
    var route = getRouteCoding(medDosage, medicationAdministrationId);

    var newObservation =
        OmopObservation.builder()
            .personId(personId)
            .observationDate(startDateTime.toLocalDate())
            .observationDatetime(startDateTime)
            .visitOccurrenceId(visitOccId)
            .observationSourceConceptId(medicationSourceConceptId)
            .observationConceptId(medicationConceptId)
            .observationTypeConceptId(CONCEPT_EHR_MEDICATION_LIST)
            .observationSourceValue(medicationCode)
            .fhirLogicalId(medicationAdministrationLogicId)
            .fhirIdentifier(medicationAdministrationSourceIdentifier)
            .build();

    if (dose != null) {
      var doseCoding = new Coding().setCode(dose.getUnit()).setSystem(fhirSystems.getUcum());

      var unitConcept =
          findOmopConcepts.getConcepts(
              doseCoding,
              startDateTime.toLocalDate(),
              bulkload,
              dbMappings,
              medicationAdministrationId);

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
              route, startDateTime.toLocalDate(), bulkload, dbMappings, medicationAdministrationId);
      newObservation.setQualifierSourceValue(routeConcept.getConceptCode());
      newObservation.setQualifierConceptId(
          routeConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
              ? null
              : routeConcept.getConceptId());
    }

    return newObservation;
  }

  /**
   * Gets the drug code from medication codeable concept or searches the drug code from the
   * referenced FHIR Medication resource in medication_id_map table in OMOP CDM.
   *
   * @param medicationReferenceLogicalId logical id of the referenced FHIR Medication resource
   * @param medicationReferenceIdentifier identifier of the referenced FHIR Medication resource
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @return ATC code from the referenced FHIR Medication resource
   */
  private Coding getMedCoding(
      String medicationReferenceLogicalId,
      String medicationReferenceIdentifier,
      MedicationAdministration srcMedicationAdministration) {

    // if the drug code is referenced use CodeableConcept in MedicationAdministration
    if (srcMedicationAdministration.hasMedicationCodeableConcept()) {
      var medicationCodeableConcept = srcMedicationAdministration.getMedicationCodeableConcept();
      if (medicationCodeableConcept.hasCoding()
          && !medicationCodeableConcept.getCoding().isEmpty()) {
        var codings =
            medicationCodeableConcept.getCoding().stream()
                .filter(med -> fhirSystems.getMedicationCodes().contains(med.getSystem()))
                .findFirst();
        if (!codings.isEmpty()) {
          return codings.get();
        }
      }
    }

    Map<String, List<MedicationIdMap>> medicationIdMap = dbMappings.getFindMedication();

    for (var entry : medicationIdMap.entrySet()) {
      var medications = entry.getValue();
      for (var med : medications) {
        var medLogicalId = med.getFhirLogicalId();
        var medIdentifier = med.getFhirIdentifier();
        if ((StringUtils.isNotBlank(medIdentifier)
                && medIdentifier.equals(medicationReferenceIdentifier))
            || (StringUtils.isNotBlank(medLogicalId)
                && medLogicalId.equals(medicationReferenceLogicalId))) {
          return new Coding().setCode(entry.getKey()).setSystem(fhirSystems.getAtc().get(0));
        }
      }
    }

    return null;
  }

  /**
   * Extract dose quantity from dosage
   *
   * @param medDosage Dosage information from MedicationAdministration resource
   * @return the quantity of dose
   */
  private Quantity getDose(
      MedicationAdministrationDosageComponent medDosage, String medicationAdministrationId) {
    if (medDosage != null && medDosage.hasDose()) {
      return medDosage.getDose();
    }
    // No dose unit available
    log.debug(
        "Unable to determine the [dose] for [MedicationAdministration]: {}.",
        medicationAdministrationId);
    invalidDoesCounter.increment();
    return null;
  }

  /**
   * Extract dosage information from MedicationAdministration resource
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @return the dosage information from MedicationAdministration resource
   */
  private MedicationAdministrationDosageComponent getDosage(
      MedicationAdministration srcMedicationAdministration, String medicationAdministrationId) {
    var dosage = srcMedicationAdministration.getDosage();
    if (dosage == null) {
      log.debug(
          "Unable to determine the [dosage] for [MedicationAdministration]: {}.",
          medicationAdministrationId);
      invalidDosageCounter.increment();
      return null;
    }
    return dosage;
  }

  /**
   * Extracts route information from FHIR MedicationAdministration resource.
   *
   * @param medDosage Dosage information from MedicationAdministration resource
   * @return route from FHIR MedicationAdministration resource
   */
  private Coding getRouteCoding(
      MedicationAdministrationDosageComponent medDosage, String medicationAdministrationId) {

    if (medDosage != null) {
      var routeCoding =
          medDosage.getRoute().getCoding().stream()
              .filter(coding -> fhirSystems.getMedicationRoute().contains(coding.getSystem()))
              .findFirst();
      if (routeCoding.isPresent()) {
        return routeCoding.get();
      }
    }

    // No route available
    log.debug(
        "Unable to determine the [route value] for [MedicationAdministration]: {}.",
        medicationAdministrationId);
    invalidRouteValueCounter.increment();
    return null;
  }

  /**
   * Deletes FHIR MedicationAdministration resources from OMOP CDM tables using fhir_logical_id and
   * fhir_identifier
   *
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @param medicationAdministrationSourceIdentifier identifier of the FHIR MedicationAdministration
   *     resource
   */
  private void deleteExistingMedicationAdministrationEntry(
      String medicationAdministrationLogicId, String medicationAdministrationSourceIdentifier) {
    if (!Strings.isNullOrEmpty(medicationAdministrationLogicId)) {
      medicationAdministrationService.deleteExistingMedAdsByFhirLogicalId(
          medicationAdministrationLogicId);
    } else {
      medicationAdministrationService.deleteExistingMedAdsByFhirIdentifier(
          medicationAdministrationSourceIdentifier);
    }
  }
}
