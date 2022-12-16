package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR_MEDICATION_LIST;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.MedicationAdministration;
import org.hl7.fhir.r4.model.MedicationAdministration.MedicationAdministrationDosageComponent;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.DrugExposure;
import org.miracum.etl.fhirtoomop.repository.service.DrugExposureMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
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
  @Autowired DrugExposureMapperServiceImpl drugExposureMapperService;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;

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
    if (bulkload.equals(Boolean.FALSE)) {
      deleteExisingDrugExposures(
          medicationAdministrationLogicId, medicationAdministrationSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info(
            "Found a deleted resource [{}]. Deleting from OMOP DB.",
            medicationAdministrationLogicId);
        return null;
      }
    }

    var personId = getPersonId(srcMedicationAdministration, medicationAdministrationLogicId);
    if (personId == null) {
      log.warn("No matching [Person] found for {}. Skip resource", medicationAdministrationLogicId);

      noPersonIdCounter.increment();
      return null;
    }

    var onset = getMedicationAdministrationOnset(srcMedicationAdministration);
    if (onset.getStartDateTime() == null) {
      log.warn(
          "Unable to determine the [datetime] for {}. Skip resource",
          medicationAdministrationLogicId);

      noStartDateCounter.increment();
      return null;
    }

    var medicationReferenceLogicalId = getMedicationReferenceLogicalId(srcMedicationAdministration);
    var medicationReferenceIdentifier =
        getMedicationReferenceIdentifier(srcMedicationAdministration);

    var medCoding =
        getMedCoding(
            medicationReferenceLogicalId,
            medicationReferenceIdentifier,
            srcMedicationAdministration);
    if (medCoding == null) {
      log.warn(
          "Unable to determine the [referenced medication code] for {}. Skip resource",
          medicationAdministrationLogicId);
      noCodeCounter.increment();
      return null;
    }

    var visitOccId =
        getVisitOccId(srcMedicationAdministration, personId, medicationAdministrationLogicId);
    var medDosage = getDosage(srcMedicationAdministration, medicationAdministrationLogicId);

    var newDrugExposure =
        createNewDrugExposure(
            medDosage,
            onset,
            personId,
            visitOccId,
            medCoding,
            medicationAdministrationLogicId,
            medicationAdministrationSourceIdentifier);

    wrapper.getDrugExposure().add(newDrugExposure);

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
      String medicationAdministrationLogicId) {
    var patientReferenceIdentifier =
        referenceUtils.getSubjectReferenceIdentifier(srcMedicationAdministration);
    var patientReferenceLogicalId =
        referenceUtils.getSubjectReferenceLogicalId(srcMedicationAdministration);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, medicationAdministrationLogicId);
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
   * @param medicationAdministrationLogicId logical id of the referenced FHIR
   *     MedicationAdministration resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(
      MedicationAdministration srcMedicationAdministration,
      Long personId,
      String medicationAdministrationLogicId) {
    var encounterReferenceIdentifier = getVisitReferenceIdentifier(srcMedicationAdministration);
    var encounterReferenceLogicalId = getVisitReferenceLogicalId(srcMedicationAdministration);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            medicationAdministrationLogicId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for {}.", medicationAdministrationLogicId);
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

  /**
   * Creates a new record of the drug_exposure table in OMOP CDM for the processed FHIR
   * MedicationAdministration resource.
   *
   * @param medDosage Dosage information from MedicationAdministration resource
   * @param onset start date time and end date time of the FHIR MedicationAdministration resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param medCoding the Coding element of medication code
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @param medicationAdministrationSourceIdentifier identifier of the FHIR MedicationAdministration
   *     resource
   * @return new record of the drug_exposure table in OMOP CDM for the processed FHIR
   *     MedicationAdministration resource
   */
  private DrugExposure createNewDrugExposure(
      MedicationAdministrationDosageComponent medDosage,
      ResourceOnset onset,
      Long personId,
      Long visitOccId,
      Coding medCoding,
      String medicationAdministrationLogicId,
      String medicationAdministrationSourceIdentifier) {

    var startDateTime = onset.getStartDateTime();
    var endDateTime = onset.getEndDateTime();

    var medCodeConcept =
        findOmopConcepts.getConcepts(medCoding, startDateTime.toLocalDate(), bulkload, dbMappings);
    if (medCodeConcept == null) {
      return null;
    }
    var dose = getDose(medDosage, medicationAdministrationLogicId);
    var route = getRouteCoding(medDosage, medicationAdministrationLogicId);

    var drugExposure =
        DrugExposure.builder()
            .drugExposureStartDate(startDateTime.toLocalDate())
            .drugExposureStartDatetime(startDateTime)
            .drugExposureEndDatetime(endDateTime)
            .drugExposureEndDate(
                endDateTime == null ? startDateTime.toLocalDate() : endDateTime.toLocalDate())
            .personId(personId)
            .drugSourceConceptId(medCodeConcept.getConceptId())
            .drugConceptId(medCodeConcept.getConceptId())
            .visitOccurrenceId(visitOccId)
            .drugTypeConceptId(CONCEPT_EHR_MEDICATION_LIST)
            .drugSourceValue(medCodeConcept.getConceptCode())
            .fhirLogicalId(medicationAdministrationLogicId)
            .fhirIdentifier(medicationAdministrationSourceIdentifier)
            .build();

    if (dose != null) {
      drugExposure.setDoseUnitSourceValue(dose.getUnit());
      drugExposure.setQuantity(dose.getValue());
    }

    if (route != null) {
      var routeConcept =
          findOmopConcepts.getConcepts(route, startDateTime.toLocalDate(), bulkload, dbMappings);
      drugExposure.setRouteSourceValue(routeConcept.getConceptCode());
      drugExposure.setRouteConceptId(
          routeConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
              ? null
              : routeConcept.getConceptId());
    }

    return drugExposure;
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

    // return the medication reference logical id if no ATC code was found
    if (bulkload.equals(Boolean.FALSE)) {
      return new Coding()
          .setCode(medicationReferenceLogicalId)
          .setSystem("http://no-medication-code-found");
    }

    return null;
  }

  /**
   * Extract dose quantity from dosage
   *
   * @param medDosage Dosage information from MedicationAdministration resource
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @return the quantity of dose
   */
  private Quantity getDose(
      MedicationAdministrationDosageComponent medDosage, String medicationAdministrationLogicId) {
    if (medDosage != null && medDosage.hasDose()) {
      return medDosage.getDose();
    }
    // No dose unit available
    log.debug("Unable to determine the [dose] for {}.", medicationAdministrationLogicId);

    return null;
  }

  /**
   * Extract dosage information from MedicationAdministration resource
   *
   * @param srcMedicationAdministration FHIR MedicationAdministration resource
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @return the dosage information from MedicationAdministration resource
   */
  private MedicationAdministrationDosageComponent getDosage(
      MedicationAdministration srcMedicationAdministration,
      String medicationAdministrationLogicId) {
    var dosage = srcMedicationAdministration.getDosage();
    if (dosage == null) {
      log.debug("Unable to determine the [dosage] for {}.", medicationAdministrationLogicId);
      return null;
    }
    return dosage;
  }

  /**
   * Extracts route information from FHIR MedicationAdministration resource.
   *
   * @param medDosage Dosage information from MedicationAdministration resource
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @return route from FHIR MedicationAdministration resource
   */
  private Coding getRouteCoding(
      MedicationAdministrationDosageComponent medDosage, String medicationAdministrationLogicId) {

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
    log.debug("Unable to determine the [route value] for {}.", medicationAdministrationLogicId);

    return null;
  }

  /**
   * Delete FHIR MedicationAdministration resources from OMOP CDM tables using fhir_logical_id and
   * fhir_identifier
   *
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationAdministration resource
   * @param medicationAdministrationSourceIdentifier identifier of the FHIR MedicationAdministration
   *     resource
   */
  private void deleteExisingDrugExposures(
      String medicationAdministrationLogicId, String medicationAdministrationSourceIdentifier) {
    if (!Strings.isNullOrEmpty(medicationAdministrationLogicId)) {
      drugExposureMapperService.deleteExistingDrugExposureByFhirLogicalId(
          medicationAdministrationLogicId);
    } else {
      drugExposureMapperService.deleteExistingDrugExposureByFhirIdentifier(
          medicationAdministrationSourceIdentifier);
    }
  }
}
