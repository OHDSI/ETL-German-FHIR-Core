package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_CLAIM;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_STATEMENT_ACCEPTABLE_STATUS_LIST;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Dosage;
import org.hl7.fhir.r4.model.Dosage.DosageDoseAndRateComponent;
import org.hl7.fhir.r4.model.MedicationStatement;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Range;
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
 * The MedicationStatementMapper class describes the business logic of transforming a FHIR
 * MedicationStatement resource to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class MedicationStatementMapper implements FhirMapper<MedicationStatement> {
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
      MapperMetrics.setNoStartDateCounter("stepProcessMedicationStatements");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessMedicationStatements");
  private static final Counter invalidCodeCounter =
      MapperMetrics.setInvalidCodeCounter("stepProcessMedicationStatements");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessMedicationStatements");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessMedicationStatements");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessMedicationStatements");

  /**
   * Constructor for objects of the class MedicationStatementMapper.
   *
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public MedicationStatementMapper(
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
   * Maps a FHIR MedicationStatement resource to drug_exposure table in OMOP CDM.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR
   *     MedicationStatement resource
   */
  @Override
  public OmopModelWrapper map(MedicationStatement srcMedicationStatement, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var medicationStatementLogicId = fhirReferenceUtils.extractId(srcMedicationStatement);

    var medicationStatementSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcMedicationStatement);

    if (Strings.isNullOrEmpty(medicationStatementLogicId)
        && Strings.isNullOrEmpty(medicationStatementSourceIdentifier)) {
      log.warn(
          "No [Identifier] or [Id] found. [MedicationStatement] resource is invalid. Skip resource.");

      noFhirReferenceCounter.increment();

      return null;
    }

    var statusElement = srcMedicationStatement.getStatusElement();
    var statusValue = checkDataAbsentReason.getValue(statusElement);
    if (Strings.isNullOrEmpty(statusValue)
        || !FHIR_RESOURCE_MEDICATION_STATEMENT_ACCEPTABLE_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status] of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          medicationStatementLogicId);
      return null;
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExisingDrugExposures(medicationStatementLogicId, medicationStatementSourceIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info(
            "Found a deleted resource [{}]. Deleting from OMOP DB.", medicationStatementLogicId);
        return null;
      }
    }

    var personId = getPersonId(srcMedicationStatement, medicationStatementLogicId);
    if (personId == null) {

      log.warn("No matching [Person] found for {}. Skip resource", medicationStatementLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var onset = getMedicationStatementOnset(srcMedicationStatement);
    if (onset.getStartDateTime() == null) {
      log.warn(
          "Unable to determine the datetime for {}. Skip resource", medicationStatementLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var medicationReferenceLogicalId = getMedicationReferenceLogicalId(srcMedicationStatement);
    var medicationReferenceIdentifier = getMedicationReferenceIdentifier(srcMedicationStatement);

    var medCoding =
        getMedCoding(
            medicationReferenceLogicalId, medicationReferenceIdentifier, srcMedicationStatement);
    if (medCoding == null) {
      log.warn(
          "Unable to determine the [medication code] for {}. Skip resource",
          medicationStatementLogicId);
      noCodeCounter.increment();
      return null;
    }

    var visitOccId = getVisitOccId(srcMedicationStatement, personId, medicationStatementLogicId);
    var medDosage = getDosage(srcMedicationStatement, medicationStatementLogicId);

    var newDrugExposures =
        createNewDrugExposure(
            medDosage,
            onset,
            personId,
            visitOccId,
            medCoding,
            medicationStatementLogicId,
            medicationStatementSourceIdentifier);

    wrapper.setDrugExposure(newDrugExposures);

    return wrapper;
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR
   * MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @param medicationStatementLogicId logical id of the FHIR MedicationStatement resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(
      MedicationStatement srcMedicationStatement, String medicationStatementLogicId) {
    var patientReferenceIdentifier =
        referenceUtils.getSubjectReferenceIdentifier(srcMedicationStatement);
    var patientReferenceLogicalId =
        referenceUtils.getSubjectReferenceLogicalId(srcMedicationStatement);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, medicationStatementLogicId);
  }

  /**
   * Extracts date time information from the FHIR MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @return start date time and end date time of the FHIR MedicationStatement resource
   */
  private ResourceOnset getMedicationStatementOnset(MedicationStatement srcMedicationStatement) {
    var resourceOnset = new ResourceOnset();
    var effective = checkDataAbsentReason.getValue(srcMedicationStatement.getEffective());
    if (effective == null) {
      return resourceOnset;
    }

    if (srcMedicationStatement.hasEffectiveDateTimeType()) {
      var effectiveDateTime =
          checkDataAbsentReason.getValue(srcMedicationStatement.getEffectiveDateTimeType());
      if (effectiveDateTime != null) {
        resourceOnset.setStartDateTime(effectiveDateTime);
        return resourceOnset;
      }
    }

    if (srcMedicationStatement.hasEffectivePeriod()) {
      var effectivePeriod =
          checkDataAbsentReason.getValue(srcMedicationStatement.getEffectivePeriod());
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
      return resourceOnset;
    }

    if (srcMedicationStatement.hasDateAsserted()
        && srcMedicationStatement.getDateAsserted() != null) {
      resourceOnset.setStartDateTime(
          new Timestamp(srcMedicationStatement.getDateAsserted().getTime()).toLocalDateTime());
    }

    return resourceOnset;
  }

  /**
   * Extracts the logical id of the referenced FHIR Medication resource for the processed FHIR
   * MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @return logical id of the referenced FHIR Medication resource
   */
  private String getMedicationReferenceLogicalId(MedicationStatement srcMedicationStatement) {
    // ID of the corresponding medication resource from medication reference
    if (srcMedicationStatement.hasMedicationReference()
        && srcMedicationStatement.getMedicationReference().hasReference()) {
      return "med-"
          + srcMedicationStatement.getMedicationReference().getReferenceElement().getIdPart();
    }

    return null;
  }

  /**
   * Extracts the identifier of the referenced FHIR Medication resource for the processed FHIR
   * MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @return identifier of the referenced FHIR Medication resource
   */
  private String getMedicationReferenceIdentifier(MedicationStatement srcMedicationStatement) {

    if (!srcMedicationStatement.hasMedicationReference()
        || srcMedicationStatement.getMedicationReference() == null) {
      return null;
    }

    if (srcMedicationStatement.getMedicationReference().hasIdentifier()
        && !srcMedicationStatement.getMedicationReference().getIdentifier().isEmpty()
        && srcMedicationStatement.getMedicationReference().getIdentifier() != null
        && srcMedicationStatement.getMedicationReference().getIdentifier().hasValue()
        && srcMedicationStatement.getMedicationReference().getIdentifier().getValue() != null) {

      return "med-" + srcMedicationStatement.getMedicationReference().getIdentifier().getValue();
    }

    return null;
  }

  /**
   * Returns the visit_occurrence_id of the referenced FHIR Encounter resource for the processed
   * FHIR MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param medicationStatementLogicId logical id of the FHIR MedicationStatement resource
   * @return visit_occurrence_id of the referenced FHIR Encounter resource from visit_occurrence
   *     table in OMOP CDM
   */
  private Long getVisitOccId(
      MedicationStatement srcMedicationStatement,
      Long personId,
      String medicationStatementLogicId) {
    var encounterReferenceIdentifier = getVisitReferenceIdentifier(srcMedicationStatement);
    var encounterReferenceLogicalId = getVisitReferenceLogicalId(srcMedicationStatement);
    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            medicationStatementLogicId);
    if (visitOccId == null) {
      log.debug("No matching [Encounter] found for {}.", medicationStatementLogicId);
    }
    return visitOccId;
  }

  /**
   * Extracts the logical id of the referenced FHIR Encounter resource (supply case/administrative
   * case) for the processed FHIR MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @return logical id of the referenced FHIR Encounter resource
   */
  private String getVisitReferenceLogicalId(MedicationStatement srcMedicationStatement) {
    var referencePath = "context.reference";
    var logicalId = fhirPath.evaluateFirst(srcMedicationStatement, referencePath, StringType.class);

    if (logicalId.isPresent()) {
      var reference = new Reference(logicalId.get().getValue());
      return "enc-" + reference.getReferenceElement().getIdPart();
    }

    return null;
  }

  /**
   * Extracts the identifier of the referenced FHIR Encounter resource (supply case/administrative
   * case) for the processed FHIR MedicationStatement resource.
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @return identifier of the referenced FHIR Encounter resource
   */
  private String getVisitReferenceIdentifier(MedicationStatement srcMedicationStatement) {
    var identifierByTypePath =
        String.format(
            "context.identifier.where(type.coding.system='%s' and type.coding.code='VN').value",
            fhirSystems.getIdentifierType());
    var identifier =
        fhirPath.evaluateFirst(srcMedicationStatement, identifierByTypePath, StringType.class);

    if (identifier.isPresent()) {
      return "enc-" + identifier.get().getValue();
    }

    return null;
  }

  /**
   * Creates a new record of the drug_exposure table in OMOP CDM for the processed FHIR
   * MedicationStatement resource.
   *
   * @param medDosages Dosage information from MedicationStatement resource
   * @param onset start date time and end date time of the FHIR MedicationStatement resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param medCoding the Coding element of medication code
   * @param medicationStatementLogicId logical id of the FHIR MedicationStatement resource
   * @param medicationStatementSourceIdentifier identifier of the FHIR MedicationStatement
   * @return new record of the drug_exposure table in OMOP CDM for the processed FHIR
   *     MedicationStatement resource
   */
  private List<DrugExposure> createNewDrugExposure(
      List<Dosage> medDosages,
      ResourceOnset onset,
      Long personId,
      Long visitOccId,
      Coding medCoding,
      String medicationStatementLogicId,
      String medicationStatementSourceIdentifier) {

    var startDateTime = onset.getStartDateTime();
    var endDateTime = onset.getEndDateTime();

    var medCodeConcept =
        findOmopConcepts.getConcepts(medCoding, startDateTime.toLocalDate(), bulkload, dbMappings);
    if (medCodeConcept == null) {
      return Collections.emptyList();
    }

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
            .drugTypeConceptId(CONCEPT_CLAIM)
            .drugSourceValue(medCodeConcept.getConceptCode())
            .fhirLogicalId(medicationStatementLogicId)
            .fhirIdentifier(medicationStatementSourceIdentifier)
            .build();

    List<DrugExposure> newDrugExposures = new ArrayList<>();

    for (var dosage : medDosages) {

      var route = getRouteCoding(dosage, medicationStatementLogicId);
      if (route != null) {

        var routeConcept =
            findOmopConcepts.getConcepts(route, startDateTime.toLocalDate(), bulkload, dbMappings);
        drugExposure.setRouteSourceValue(routeConcept.getConceptCode());
        drugExposure.setRouteConceptId(
            routeConcept.getConceptId() == CONCEPT_NO_MATCHING_CONCEPT
                ? null
                : routeConcept.getConceptId());
      }
      var doseAndRateComponents = getDoseAndRateComponents(dosage, medicationStatementLogicId);
      if (!doseAndRateComponents.isEmpty()) {
        var quantity = getDoseQuantity(doseAndRateComponents);
        var range = getDoseRange(doseAndRateComponents);
        if (quantity != null) {
          drugExposure.setQuantity(quantity.getValue());
          drugExposure.setDoseUnitSourceValue(quantity.getCode());
        }
        if (range != null) {
          drugExposure.setQuantity(getRangeMeanValue(range.getLow(), range.getHigh()));
          drugExposure.setDoseUnitSourceValue(getRangeUnit(range.getLow(), range.getHigh()));
        }
      }
      addToList(newDrugExposures, drugExposure);
    }
    return newDrugExposures;
  }

  /**
   * Calculate the mean value of range using low quantity and high quantity
   *
   * @param low low quantity
   * @param high high quantity
   * @return the mean value of range
   */
  private BigDecimal getRangeMeanValue(Quantity low, Quantity high) {
    if (low.isEmpty() && high.isEmpty()) {
      return BigDecimal.ZERO;
    }
    var lowValue = BigDecimal.ZERO;
    var highValue = BigDecimal.ZERO;

    if (!low.isEmpty()) {
      lowValue = low.getValue();
    }
    if (!high.isEmpty()) {
      highValue = high.getValue();
    }
    return BigDecimal.valueOf((lowValue.add(highValue)).doubleValue() / 2);
  }

  /**
   * Extract the unit of range using low quantity and high quantity
   *
   * @param low low quantity
   * @param high high quantity
   * @return the unit of range
   */
  private String getRangeUnit(Quantity low, Quantity high) {
    if (low.isEmpty() && high.isEmpty()) {
      return null;
    }
    String rangeUnit = null;
    if (!low.isEmpty()) {
      rangeUnit = low.getUnit();
    }
    if (!high.isEmpty()) {
      rangeUnit = high.getUnit();
    }
    return rangeUnit;
  }

  /**
   * Add new entry of DRUG_EXPOSURE to a list without duplicate
   *
   * @param newDrugExposures a list of entry of DRUG_EXPOSURE
   * @param drugExposure new entry of DRUG_EXPOSURE
   */
  private void addToList(List<DrugExposure> newDrugExposures, DrugExposure drugExposure) {
    if (!newDrugExposures.contains(drugExposure)) {
      newDrugExposures.add(drugExposure);
    }
  }

  /**
   * Gets the drug code from medication codeable concept or searches the drug code from the
   * referenced FHIR Medication resource in medication_id_map table in OMOP CDM.
   *
   * @param medicationReferenceLogicalId logical id of the referenced FHIR Medication resource
   * @param medicationReferenceIdentifier identifier of the referenced FHIR Medication resource
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @return drug code from the referenced FHIR Medication resource
   */
  private Coding getMedCoding(
      String medicationReferenceLogicalId,
      String medicationReferenceIdentifier,
      MedicationStatement srcMedicationStatement) {

    // if the drug code is referenced use CodeableConcept in MedicationStatement
    if (srcMedicationStatement.hasMedicationCodeableConcept()) {

      var medicationCodeableConcept = srcMedicationStatement.getMedicationCodeableConcept();
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

    Map<String, List<MedicationIdMap>> medicationMap = dbMappings.getFindMedication();

    for (var entry : medicationMap.entrySet()) {
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
   * Extracts route information from FHIR MedicationStatement resource.
   *
   * @param medDosage Dosage information from MedicationStatement resource
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationStatement resource
   * @return route from FHIR MedicationStatement resource
   */
  private Coding getRouteCoding(Dosage medDosage, String medicationAdministrationLogicId) {
    // Route from dosage

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
   * Extract dosage information from MedicationStatement resource as list
   *
   * @param srcMedicationStatement FHIR MedicationStatement resource
   * @param medicationStatementSourceId logical id of the FHIR MedicationStatement resource
   * @return the dosage information from MedicationStatement resource
   */
  private List<Dosage> getDosage(
      MedicationStatement srcMedicationStatement, String medicationStatementSourceId) {
    if (srcMedicationStatement.hasDosage()) {
      return srcMedicationStatement.getDosage();
    }
    log.debug("Unable to determine the [dosage] for {}.", medicationStatementSourceId);
    return Collections.emptyList();
  }

  /**
   * Extract doseAndRate from dosage as a list
   *
   * @param medDosage Dosage information from MedicationAdministration resource
   * @param medicationStatementSourceId logical id of the FHIR MedicationStatement resource
   * @return a list of doseAndRate
   */
  private List<DosageDoseAndRateComponent> getDoseAndRateComponents(
      Dosage medDosage, String medicationStatementSourceId) {
    if (medDosage != null && medDosage.hasDoseAndRate()) {
      return medDosage.getDoseAndRate();
    }
    log.debug("Unable to determine the [dose] for {}.", medicationStatementSourceId);
    return Collections.emptyList();
  }

  /**
   * Extract dose quantity from doseAndRate
   *
   * @param doseAndRateComponents a list of doseAndRate
   * @return dose quantity
   */
  private Quantity getDoseQuantity(List<DosageDoseAndRateComponent> doseAndRateComponents) {

    var component =
        doseAndRateComponents.stream()
            .filter(DosageDoseAndRateComponent::hasDoseQuantity)
            .findFirst();
    if (component.isPresent()) {
      var doseQuantity = component.get().getDoseQuantity();
      if (!doseQuantity.isEmpty()) {
        return doseQuantity;
      }
    }
    return null;
  }

  /**
   * Extract range from doseAndRate
   *
   * @param doseAndRateComponents a list of doseAndRate
   * @return range
   */
  private Range getDoseRange(List<DosageDoseAndRateComponent> doseAndRateComponents) {

    var component =
        doseAndRateComponents.stream().filter(DosageDoseAndRateComponent::hasDoseRange).findFirst();
    if (component.isPresent()) {
      var doseRange = component.get().getDoseRange();
      if (!doseRange.isEmpty()) {
        return doseRange;
      }
    }
    return null;
  }

  /**
   * Delete FHIR MedicationStatement resources from OMOP CDM tables using fhir_logical_id and
   * fhir_identifier
   *
   * @param medicationAdministrationLogicId logical id of the FHIR MedicationStatement resource
   * @param medicationAdministrationSourceIdentifier identifier of the FHIR MedicationStatement
   *     resource
   */
  private void deleteExisingDrugExposures(
      String medicationStatementLogicId, String medicationStatementSourceIdentifier) {
    if (!Strings.isNullOrEmpty(medicationStatementLogicId)) {
      drugExposureMapperService.deleteExistingDrugExposureByFhirLogicalId(
          medicationStatementLogicId);
    } else {
      drugExposureMapperService.deleteExistingDrugExposureByFhirIdentifier(
          medicationStatementSourceIdentifier);
    }
  }
}
