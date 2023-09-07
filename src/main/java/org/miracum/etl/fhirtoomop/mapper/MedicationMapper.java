package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_MEDICATION_ACCEPTABLE_STATUS_LIST;

import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Medication;
import org.miracum.etl.fhirtoomop.IIdMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.miracum.etl.fhirtoomop.repository.service.OmopConceptServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The MedicationMapper class describes the business logic of transforming a FHIR Medication
 * resource to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class MedicationMapper implements FhirMapper<Medication> {
  private static final AtomicLong newMedicationId = new AtomicLong(1);
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final IIdMappings idMappings;
  private final OmopRepository repositories;
  private final Boolean bulkload;

  @Autowired OmopConceptServiceImpl omopConceptService;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;

  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessMedications");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessMedications");

  /**
   * Constructor for objects of the class MedicationMapper.
   *
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param idMappings reference to internal id mappings
   * @param repositories OMOP CDM repositories
   */
  @Autowired
  public MedicationMapper(Boolean bulkload, IIdMappings idMappings, OmopRepository repositories) {

    this.bulkload = bulkload;
    this.idMappings = idMappings;
    this.repositories = repositories;
  }

  /**
   * Maps a FHIR Medication resource to medication_id_map table in OMOP CDM.
   *
   * @param srcMedication FHIR Medication resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Medication
   *     resource
   */
  @Override
  public OmopModelWrapper map(Medication srcMedication, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var medicationLogicId = fhirReferenceUtils.extractId(srcMedication);
    var medicationSourceIdentifier =
        fhirReferenceUtils.extractResourceFirstIdentifier(srcMedication);
    if (Strings.isNullOrEmpty(medicationLogicId)
        && Strings.isNullOrEmpty(medicationSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Medication] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }

    String medicationId = "";
    if (!Strings.isNullOrEmpty(medicationLogicId)) {
      medicationId = srcMedication.getId();
    }

    var statusValue = getStatusValue(srcMedication);
    if (!Strings.isNullOrEmpty(statusValue)
        && !FHIR_RESOURCE_MEDICATION_ACCEPTABLE_STATUS_LIST.contains(statusValue)) {
      log.error(
          "The [status]: {} of {} is not acceptable for writing into OMOP CDM. Skip resource.",
          statusValue,
          medicationId);
      return null;
    }

    var atcCode = getAtcCode(srcMedication);
    if (atcCode == null) {
      log.warn("No [ATC] code found for [Medication]: {}. Skip resource", medicationId);
      noCodeCounter.increment();
      return null;
    }

    var medicationIdMap =
        omopReferenceUtils.createMedicationIdMap(
            ResourceType.MEDICATION.name(), medicationLogicId, medicationSourceIdentifier, atcCode);

    var newMedicationFhirOmopId =
        getExistingMedicationOmopId(medicationLogicId, medicationSourceIdentifier);

    medicationIdMap.setFhirOmopId(newMedicationFhirOmopId);

    setMedication(medicationIdMap, wrapper);

    return wrapper;
  }

  /**
   * Sets the new records from the FHIR Medication resource for the write process to OMOP CDM.
   * During incremental loading, the records are written directly to OMOP CDM. During bulk load, the
   * records are cached in the OmopModelWrapper for subsequent writing.
   *
   * @param medicationIdMap new record of the medication_id_map table in OMOP CDM for the processed
   *     FHIR Medication resource
   * @param wrapper cache of newly created OMOP CDM records from the FHIR Medication resource
   */
  private void setMedication(MedicationIdMap medicationIdMap, OmopModelWrapper wrapper) {
    if (bulkload.equals(Boolean.FALSE)) {

      medicationIdMap = omopConceptService.saveAndFlush(medicationIdMap);
      return;
    }
    wrapper.getMedicationIdMap().add(medicationIdMap);
  }

  /**
   * Extracts the status value from the FHIR Medication resource.
   *
   * @param srcMedication FHIR Medication resource
   * @return status value from the FHIR Medication resource
   */
  private String getStatusValue(Medication srcMedication) {
    var medicationStatus = srcMedication.getStatusElement();
    if (medicationStatus == null) {
      return null;
    }
    var statusValue = medicationStatus.getCode();
    if (!Strings.isNullOrEmpty(statusValue)) {
      return statusValue;
    }
    return null;
  }

  /**
   * Extracts the ATC code from the FHIR Medication resource.
   *
   * @param srcMedication FHIR Medication resource
   * @return ATC code from the FHIR Medication resource
   */
  private String getAtcCode(Medication srcMedication) {
    // Extracts the ATC coding
    var atcCoding =
        srcMedication.getCode().getCoding().stream()
            //            .filter(code -> code.getSystem().equalsIgnoreCase(fhirSystems.getAtc()))
            .filter(code -> fhirSystems.getAtc().contains(code.getSystem()))
            .findFirst();

    if (atcCoding.isEmpty() || atcCoding.get() == null) {
      return null;
    }

    if (!atcCoding.get().hasCode() || atcCoding.get().getCode() == null) {
      return null;
    }

    return atcCoding.get().getCode();
  }

  /**
   * Returns the fhir_omop_id for the processed FHIR Medication resource in OMOP CDM.
   *
   * @param medicationLogicId logical id of the FHIR Medication resource
   * @param medicationSourceIdentifier identifier of the FHIR Medication resource
   * @return fhir_omop_id for the processed FHIR Medication resource in OMOP CDM
   */
  private Long getExistingMedicationOmopId(
      String medicationLogicId, String medicationSourceIdentifier) {
    if (Boolean.FALSE.equals(bulkload)) {
      var existingMedicationId =
          getExistingMedicationId(medicationLogicId, medicationSourceIdentifier);
      if (existingMedicationId == null) {
        return getMedicationFhirOmopId();
      }

      return existingMedicationId;

    } else {
      return idMappings
          .getMedicationIds()
          .getOrCreateIfAbsent(medicationSourceIdentifier + ":" + medicationLogicId);
    }
  }

  /**
   * Searches if a FHIR Medication resource already exists in medication_id_map table in OMOP CDM.
   *
   * @param medicationLogicId logical id of the FHIR Medication resource
   * @param medicationSourceIdentifier identifier of the FHIR Medication resource
   * @return list of fhir_omop_ids for existing medication_id_map records
   */
  private Long getExistingMedicationId(
      String medicationLogicId, String medicationSourceIdentifier) {
    if (!Strings.isNullOrEmpty(medicationLogicId)) {
      return getExistingMedicationByLogicalId(medicationLogicId);
    } else {
      return getExistingMedicationByIdentifier(medicationSourceIdentifier);
    }
  }

  /**
   * Searches if a FHIR Medication resource already exists in medication_id_map table in OMOP CDM
   * based on the logical id of the FHIR Medication resource.
   *
   * @param logicalId logical id of the FHIR Condition resource
   * @return fhir_omop_id for existing medication_id_map record based on the logical id of the FHIR
   *     Medication resource
   */
  private Long getExistingMedicationByLogicalId(String logicalId) {
    var medicationsLogicalId =
        repositories.getMedicationIdRepository().findByFhirLogicalId(logicalId);
    if (!medicationsLogicalId.isEmpty()) {
      return medicationsLogicalId.get(0).getFhirOmopId();
    }

    return null;
  }

  /**
   * Searches if a FHIR Medication resource already exists in medication_id_map table in OMOP CDM
   * based on the identifier of the FHIR Medication resource.
   *
   * @param identifier identifier of the FHIR Condition resource
   * @return fhir_omop_id for existing medication_id_map record based on the identifier of the FHIR
   *     Medication resource
   */
  private Long getExistingMedicationByIdentifier(String identifier) {
    var medicationsIdentifier =
        repositories.getMedicationIdRepository().findByFhirIdentifier(identifier);
    if (!medicationsIdentifier.isEmpty()) {
      return medicationsIdentifier.get(0).getFhirOmopId();
    }

    return null;
  }

  /**
   * Creates a new fhir_omop_id for the processed FHIR Medication resource.
   *
   * @return new fhir_omop_id for the processed FHIR Medication resource
   */
  private Long getMedicationFhirOmopId() {
    var maxMedicationFhirOmopId = repositories.getMedicationIdRepository().getMaxMedicationId();
    var medicationFhirOmopId = maxMedicationFhirOmopId == null ? 0L : maxMedicationFhirOmopId;

    return medicationFhirOmopId + newMedicationId.getAndIncrement();
  }

  // TODO: ingredient
}
