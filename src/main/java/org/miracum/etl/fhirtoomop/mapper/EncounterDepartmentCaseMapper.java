package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_INPATIENT;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_STILL_PATIENT;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_VISIT_DETAIL_STATUS;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_VISIT_STATUS;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_VISIT_TYPE;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Encounter.EncounterLocationComponent;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.StringType;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOnset;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.VisitDetail;
import org.miracum.etl.fhirtoomop.repository.service.EncounterDepartmentCaseMapperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Component;

/**
 * The EncounterDepartmentCaseMapper class describes the business logic of transforming a FHIR
 * Encounter resource (department case) to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class EncounterDepartmentCaseMapper implements FhirMapper<Encounter> {

  private final ResourceFhirReferenceUtils referenceUtils;
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final IFhirPath fhirPath;
  private final Boolean bulkload;
  private final DbMappings dbMappings;

  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired EncounterDepartmentCaseMapperServiceImpl departmentCaseMapperService;
  @Autowired FindOmopConcepts findOmopConcepts;

  private static final Counter noStartDateCounter =
      MapperMetrics.setNoStartDateCounter("stepProcessEncounterDepartmentCase");
  private static final Counter noPersonIdCounter =
      MapperMetrics.setNoPersonIdCounter("stepProcessEncounterDepartmentCase");
  private static final Counter noCodeCounter =
      MapperMetrics.setNoCodeCounter("stepProcessEncounterDepartmentCase");
  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessEncounterDepartmentCase");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessEncounterDepartmentCase");

  /**
   * Constructor for objects of the class EncounterDepartmentCaseMapper.
   *
   * @param referenceUtils utilities for the identification of FHIR resource references
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public EncounterDepartmentCaseMapper(
      ResourceFhirReferenceUtils referenceUtils,
      IFhirPath fhirPath,
      Boolean bulkload,
      DbMappings dbMappings) {
    this.referenceUtils = referenceUtils;
    this.fhirPath = fhirPath;

    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR Encounter resource (department case) to visit_detail table in OMOP CDM.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Encounter
   *     resource
   */
  @Override
  public OmopModelWrapper map(Encounter srcDepartmentCaseEncounter, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var departmentCaseLogicId = fhirReferenceUtils.extractId(srcDepartmentCaseEncounter);

    var departmentCaseIdentifier =
        fhirReferenceUtils.extractIdentifier(srcDepartmentCaseEncounter, "VN");
    if (Strings.isNullOrEmpty(departmentCaseLogicId)
        && Strings.isNullOrEmpty(departmentCaseIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Encounter] resource is invalid. Skip resource.");
      noFhirReferenceCounter.increment();
      return null;
    }
    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingVisitDetails(departmentCaseLogicId, departmentCaseIdentifier);
      if (isDeleted) {
        deletedFhirReferenceCounter.increment();
        log.info("Found a deleted resource [{}]. Deleting from OMOP DB.", departmentCaseLogicId);
        return null;
      }
    }
    var personId = getPersonId(srcDepartmentCaseEncounter, departmentCaseLogicId);
    if (personId == null) {
      log.warn(
          "No matching [Person] found for [Encounter]:{}. Skip resource", departmentCaseLogicId);
      noPersonIdCounter.increment();
      return null;
    }

    var encounterReferenceIdentifier = getVisitReferenceIdentifier(srcDepartmentCaseEncounter);
    var encounterReferenceLogicalId = getVisitReferenceLogicalId(srcDepartmentCaseEncounter);
    if (encounterReferenceIdentifier == null && encounterReferenceLogicalId == null) {
      log.warn(
          "Unable to extract encounter reference from [Encounter]:{}. Skip resource",
          departmentCaseLogicId);
      return null;
    }

    var visitOccId =
        omopReferenceUtils.getVisitOccId(
            encounterReferenceIdentifier,
            encounterReferenceLogicalId,
            personId,
            srcDepartmentCaseEncounter.getId());
    if (visitOccId == null) {
      log.error(
          "No matching [VisitOccurrence] found for [Encounter]:{}. Skip resource",
          departmentCaseLogicId);
      return null;
    }

    var departmentCaseOnset =
        getDepartmentCaseOnset(srcDepartmentCaseEncounter, departmentCaseLogicId);

    if (departmentCaseOnset.getStartDateTime() == null) {
      log.warn("No [start_date] found for {}. Skip resource", departmentCaseLogicId);
      noStartDateCounter.increment();
      return null;
    }

    var newVisitDetails =
        createNewVisitDetail(
            srcDepartmentCaseEncounter,
            personId,
            visitOccId,
            departmentCaseOnset,
            departmentCaseLogicId,
            departmentCaseIdentifier);

    if (newVisitDetails.isEmpty()) {
      return null;
    }
    wrapper.setVisitDetail(newVisitDetails);
    return wrapper;
  }

  /**
   * Returns the person_id of the referenced FHIR Patient resource for the processed FHIR Encounter
   * resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return person_id of the referenced FHIR Patient resource from person table in OMOP CDM
   */
  private Long getPersonId(Encounter srcDepartmentCaseEncounter, String departmentCaseLogicId) {
    var patientReferenceIdentifier =
        referenceUtils.getSubjectReferenceIdentifier(srcDepartmentCaseEncounter);
    var patientReferenceLogicalId =
        referenceUtils.getSubjectReferenceLogicalId(srcDepartmentCaseEncounter);

    return omopReferenceUtils.getPersonId(
        patientReferenceIdentifier, patientReferenceLogicalId, departmentCaseLogicId);
  }

  /**
   * Extracts the identifier of the referenced FHIR Encounter resource (supply case/administrative
   * case) for the processed FHIR Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @return identifier of the referenced FHIR Encounter resource
   */
  private String getVisitReferenceIdentifier(Encounter srcDepartmentCaseEncounter) {
    var identifierByTypePath =
        String.format(
            "partOf.identifier.where(type.coding.system='%s' and type.coding.code='VN').value",
            fhirSystems.getIdentifierType());
    var identifier =
        fhirPath.evaluateFirst(srcDepartmentCaseEncounter, identifierByTypePath, StringType.class);

    if (identifier.isPresent()) {
      return "enc-" + identifier.get().getValue();
    }

    return null;
  }

  /**
   * Extracts the logical id of the referenced FHIR Encounter resource (supply case/administrative
   * case) for the processed FHIR Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @return logical id of the referenced FHIR Encounter resource
   */
  private String getVisitReferenceLogicalId(Encounter srcDepartmentCaseEncounter) {
    var referencePath = "partOf.reference";
    var logicalId =
        fhirPath.evaluateFirst(srcDepartmentCaseEncounter, referencePath, StringType.class);

    if (logicalId.isPresent()) {
      var reference = new Reference(logicalId.get().getValue());
      return "enc-" + reference.getReferenceElement().getIdPart();
    }

    return null;
  }

  /**
   * Maps the processed FHIR Encounter resource to visit_detail table in OMOP CDM depending on the
   * FHIR specification of the FHIR Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param departmentCaseOnset start date time and end date time of the FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @param departmentCaseLogicIdentifier identifier of the FHIR Encounter resource
   * @param wrapper cache of newly created OMOP CDM records from the FHIR Encounter resource
   */
  private List<VisitDetail> createNewVisitDetail(
      Encounter srcDepartmentCaseEncounter,
      Long personId,
      Long visitOccId,
      ResourceOnset departmentCaseOnset,
      String departmentCaseLogicId,
      String departmentCaseLogicIdentifier) {
    var stations = getStation(srcDepartmentCaseEncounter, departmentCaseLogicId);

    var fabCode = getFabCode(srcDepartmentCaseEncounter, departmentCaseLogicId);

    // Work around for p21 Data format
    if (stations.isEmpty()) {
      return createVisitDetailFromDepartmentCase(
          departmentCaseOnset,
          personId,
          visitOccId,
          srcDepartmentCaseEncounter,
          departmentCaseLogicId,
          departmentCaseLogicIdentifier,
          fabCode);
    } else {
      return createVisitDetailFromLocation(
          stations,
          personId,
          visitOccId,
          srcDepartmentCaseEncounter,
          departmentCaseLogicId,
          departmentCaseLogicIdentifier,
          fabCode);
    }
  }

  /**
   * Creates a new record of the visit_detail table in OMOP CDM for the processed FHIR Encounter
   * resource based on department case information.
   *
   * @param departmentCaseOnset start date time and end date time of the FHIR Encounter resource
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @param departmentCaseLogicIdentifier identifier of the FHIR Encounter resource
   * @param fabCode department (FAB) code of the FHIR Encounter resource
   * @param wrapper cache of newly created OMOP CDM records from the FHIR Encounter resource
   */
  private List<VisitDetail> createVisitDetailFromDepartmentCase(
      ResourceOnset departmentCaseOnset,
      Long personId,
      Long visitOccId,
      Encounter srcDepartmentCaseEncounter,
      String departmentCaseLogicId,
      String departmentCaseLogicIdentifier,
      String fabCode) {
    List<VisitDetail> visitDetailList = new ArrayList<>();
    var endDateTime = departmentCaseOnset.getEndDateTime();
    var startDateTime = departmentCaseOnset.getStartDateTime();
    var newVisitDetail =
        VisitDetail.builder()
            .personId(personId)
            .visitOccurrenceId(visitOccId)
            .careSiteId(getCareSiteId(fabCode))
            .visitDetailConceptId(
                getPatientVisitType(srcDepartmentCaseEncounter, departmentCaseLogicId))
            .fhirLogicalId(departmentCaseLogicId)
            .fhirIdentifier(departmentCaseLogicIdentifier)
            .build();
    newVisitDetail.setVisitDetailTypeConceptId(
        getVisitDetailTypeConceptId(
            null, srcDepartmentCaseEncounter, endDateTime, departmentCaseLogicId));
    newVisitDetail.setVisitDetailStartDate(startDateTime.toLocalDate());
    newVisitDetail.setVisitDetailStartDatetime(startDateTime);

    setEndDate(newVisitDetail, endDateTime, departmentCaseLogicId);

    visitDetailList.add(newVisitDetail);

    return visitDetailList;
  }

  /**
   * Creates a new record of the visit_detail table in OMOP CDM for the processed FHIR Encounter
   * resource based on location information.
   *
   * @param stations list of stations where the patient has been during this encounter
   * @param personId person_id of the referenced FHIR Patient resource
   * @param visitOccId visit_occurrence_id of the referenced FHIR Encounter resource
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @param departmentCaseLogicIdentifier identifier of the FHIR Encounter resource
   * @param fabCode FAB code of the FHIR Encounter resource
   * @param wrapper cache of newly created OMOP CDM records from the FHIR Encounter resource
   */
  private List<VisitDetail> createVisitDetailFromLocation(
      List<EncounterLocationComponent> stations,
      Long personId,
      Long visitOccId,
      Encounter srcDepartmentCaseEncounter,
      String departmentCaseLogicId,
      String departmentCaseLogicIdentifier,
      String fabCode) {

    List<VisitDetail> visitDetailList = new ArrayList<>();
    for (var station : stations) {
      var locationOnset = getStationOnset(station);

      var locationStartDateTime = locationOnset.getStartDateTime();
      var locationEndDateTime = locationOnset.getEndDateTime();
      if (locationStartDateTime == null) {

        log.warn(
            "No [start_date] found for [Location] in {}. Skip the [Location].",
            srcDepartmentCaseEncounter.getId());

        continue;
      }

      var newVisitDetail =
          VisitDetail.builder()
              .personId(personId)
              .visitOccurrenceId(visitOccId)
              .careSiteId(getCareSiteId(fabCode))
              .visitDetailConceptId(
                  getPatientVisitType(srcDepartmentCaseEncounter, departmentCaseLogicId))
              .fhirLogicalId(departmentCaseLogicId)
              .fhirIdentifier(departmentCaseLogicIdentifier)
              .build();
      newVisitDetail.setVisitDetailTypeConceptId(
          getVisitDetailTypeConceptId(
              station, srcDepartmentCaseEncounter, locationEndDateTime, departmentCaseLogicId));
      newVisitDetail.setVisitDetailStartDate(locationStartDateTime.toLocalDate());
      newVisitDetail.setVisitDetailStartDatetime(locationStartDateTime);
      newVisitDetail.setVisitDetailSourceValue(getStationName(station));

      setEndDate(newVisitDetail, locationEndDateTime, departmentCaseLogicId);

      visitDetailList.add(newVisitDetail);
    }
    return visitDetailList;
  }

  /**
   * Extracts station information from FHIR Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return list of stations where the patient has been during this encounter
   */
  private List<EncounterLocationComponent> getStation(
      Encounter srcDepartmentCaseEncounter, String departmentCaseLogicId) {
    if (!srcDepartmentCaseEncounter.hasLocation()) {
      log.warn(
          "No [Location Reference] found. [Encounter]:{} is invalid. Please check.",
          departmentCaseLogicId);
      return Collections.emptyList();
    }
    return srcDepartmentCaseEncounter.getLocation();
  }

  /**
   * Extracts date time information from the FHIR Encounter resource for a specific station.
   *
   * @param station station where the patient has been during this encounter
   * @return start date time and end date time of the station from FHIR Encounter resource
   */
  private ResourceOnset getStationOnset(EncounterLocationComponent station) {
    var period = station.getPeriod();
    LocalDateTime startDateTime;
    LocalDateTime endDateTime;
    if (period.hasStart() && period.getStart() != null) {
      startDateTime = new Timestamp(period.getStart().getTime()).toLocalDateTime();
    } else {
      startDateTime = null;
    }
    if (period.hasEnd() && period.getEnd() != null) {
      endDateTime = new Timestamp(period.getEnd().getTime()).toLocalDateTime();
    } else {
      endDateTime = null;
    }
    return new ResourceOnset(startDateTime, endDateTime);
  }

  /**
   * Extracts date time information from the FHIR Encounter resource for the department case.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return start date time and end date time of the department case from FHIR Encounter resource
   */
  private ResourceOnset getDepartmentCaseOnset(
      Encounter srcDepartmentCaseEncounter, String departmentCaseLogicId) {
    var resourceOnset = new ResourceOnset();
    if (!srcDepartmentCaseEncounter.hasPeriod() || srcDepartmentCaseEncounter.getPeriod() == null) {
      return resourceOnset;
    }
    var period = srcDepartmentCaseEncounter.getPeriod();
    if (period.getStart() != null) {
      resourceOnset.setStartDateTime(new Timestamp(period.getStart().getTime()).toLocalDateTime());
    }
    if (period.getEnd() != null) {
      resourceOnset.setEndDateTime(new Timestamp(period.getEnd().getTime()).toLocalDateTime());
    }
    return resourceOnset;
  }

  /**
   * Extracts department (FAB) code information from FHIR Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return department (FAB) code from FHIR Encounter resource
   */
  private String getFabCode(Encounter srcDepartmentCaseEncounter, String departmentCaseLogicId) {
    var fabSourceValue =
        srcDepartmentCaseEncounter.getServiceType().getCoding().stream()
            .filter(fab -> fab.getSystem().equalsIgnoreCase(fhirSystems.getDepartment()))
            .findAny();
    if (fabSourceValue.isEmpty()) {
      log.debug("No department (FAB) code found for [Encounter]:{}.", departmentCaseLogicId);
      return null;
    }
    return fabSourceValue.get().getCode();
  }

  /**
   * Maps status of the FHIR Encounter resource to OMOP CDM concept.
   *
   * @param station station where the patient has been during this encounter
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param endDateTime end date time of the FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return visit_detail_type_concept_id of the status from the FHIR Encounter resource in OMOP CDM
   */
  private Integer getVisitDetailTypeConceptId(
      @Nullable EncounterLocationComponent station,
      Encounter srcDepartmentCaseEncounter,
      LocalDateTime endDateTime,
      String departmentCaseLogicId) {

    if (station == null) {
      return getDepartmentCaseStatus(
          srcDepartmentCaseEncounter, endDateTime, departmentCaseLogicId);
    }

    return getLocationStatus(station, endDateTime, departmentCaseLogicId);
  }

  /**
   * Searches the visit_detail_type_concept_id in OMOP CDM for the extracted department case status
   * from the FHIR Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param endDateTime end date time of the FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return visit_detail_type_concept_id of department case status in OMOP CDM
   */
  private Integer getDepartmentCaseStatus(
      Encounter srcDepartmentCaseEncounter,
      LocalDateTime endDateTime,
      String departmentCaseLogicId) {
    if (srcDepartmentCaseEncounter.hasStatus() && srcDepartmentCaseEncounter.getStatus() != null) {
      var departmentCaseStatus =
          new Coding(null, srcDepartmentCaseEncounter.getStatus().toString(), null);
      var sourceToConceptMap =
          findOmopConcepts.getCustomConcepts(
              departmentCaseStatus, SOURCE_VOCABULARY_ID_VISIT_STATUS, dbMappings);
      var visitTypeConceptId = sourceToConceptMap.getTargetConceptId();
      if (!visitTypeConceptId.equals(CONCEPT_NO_MATCHING_CONCEPT)) {
        return visitTypeConceptId;
      }
      return CONCEPT_EHR;
    } else {
      if (endDateTime == null) {
        return CONCEPT_STILL_PATIENT;
      } else {
        return CONCEPT_EHR;
      }
    }
  }

  /**
   * Searches the visit_detail_type_concept_id in OMOP CDM for the extracted station status from the
   * FHIR Encounter resource.
   *
   * @param station station where the patient has been during this encounter
   * @param endDateTime end date time of the station
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return visit_detail_type_concept_id of station status in OMOP CDM
   */
  private Integer getLocationStatus(
      EncounterLocationComponent station, LocalDateTime endDateTime, String departmentCaseLogicId) {
    if (station.hasStatus() && station.getStatus() != null) {
      var locationStatus = new Coding(null, station.getStatus().toString(), null);

      var sourceToConceptMap =
          findOmopConcepts.getCustomConcepts(
              locationStatus, SOURCE_VOCABULARY_ID_VISIT_DETAIL_STATUS, dbMappings);
      var visitTypeConceptId = sourceToConceptMap.getTargetConceptId();
      if (!visitTypeConceptId.equals(CONCEPT_NO_MATCHING_CONCEPT)) {
        return visitTypeConceptId;
      }
      return CONCEPT_EHR;
    } else {
      if (endDateTime == null) {
        return CONCEPT_STILL_PATIENT;
      }
    }
    return CONCEPT_EHR;
  }

  /**
   * Searches the care_site_id from care_site table in OMOP CDM for a specific department (FAB)
   * code.
   *
   * @param fabCode department (FAB) code
   * @return care_site_id from care_site table of a specific department (FAB) code
   */
  private Long getCareSiteId(String fabCode) {

    if (Strings.isNullOrEmpty(fabCode)) {
      return null;
    }

    var careSiteMap = dbMappings.getFindCareSiteId();
    if (!careSiteMap.containsKey(fabCode)) {
      return null;
    }
    return careSiteMap.get(fabCode).getCareSiteId();
  }

  /**
   * Extracts the name of a station from FHIR Encounter resource.
   *
   * @param station station where the patient has been during this encounter
   * @return station name from FHIR Encounter resource
   */
  private String getStationName(EncounterLocationComponent station) {
    if (station == null) {
      return null;
    }
    var locationReference = station.getLocation();

    if (locationReference.hasIdentifier()) {
      var identifier = locationReference.getIdentifier();
      if (!identifier.isEmpty()) {
        return identifier.getValue();
      }
    }

    if (locationReference.hasReference()) {
      return locationReference.getReferenceElement().getIdPart();

    } else {
      return null;
    }
  }

  /**
   * Searches the visit_detail_concept_id in OMOP CDM for the extracted class from the FHIR
   * Encounter resource.
   *
   * @param srcDepartmentCaseEncounter FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @return visit_detail_concept_id of Encounter class in OMOP CDM
   */
  private Integer getPatientVisitType(
      Encounter srcDepartmentCaseEncounter, String departmentCaseLogicId) {

    if (!srcDepartmentCaseEncounter.hasClass_()
        || Strings.isNullOrEmpty(srcDepartmentCaseEncounter.getClass_().getCode())) {
      log.debug("No class found for Encounter {}.", departmentCaseLogicId);
      return CONCEPT_NO_MATCHING_CONCEPT;
    }
    var visitType = srcDepartmentCaseEncounter.getClass_().getCode();
    if (visitType.equalsIgnoreCase("station") || visitType.equalsIgnoreCase("stationaer")) {
      return CONCEPT_INPATIENT;
    }
    var sourceToConceptMap =
        findOmopConcepts.getCustomConcepts(
            new Coding(null, visitType, null), SOURCE_VOCABULARY_ID_VISIT_TYPE, dbMappings);
    return sourceToConceptMap.getTargetConceptId();
  }

  /**
   * Sets the end date and time of the new record of the visit_detail table in OMOP CDM.
   *
   * @param newVisitDetail new record of the visit_detail table in OMOP CDM for the processed FHIR
   *     Encounter resource
   * @param endDateTime end date time of the FHIR Encounter resource
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   */
  private void setEndDate(
      VisitDetail newVisitDetail, LocalDateTime endDateTime, String departmentCaseLogicId) {
    if (endDateTime != null) {
      newVisitDetail.setVisitDetailEndDate(endDateTime.toLocalDate());
      newVisitDetail.setVisitDetailEndDatetime(endDateTime);
      return;
    }
    if (newVisitDetail.getVisitDetailTypeConceptId().equals(CONCEPT_STILL_PATIENT)) {
      newVisitDetail.setVisitDetailEndDate(LocalDate.now());
      newVisitDetail.setVisitDetailEndDatetime(LocalDateTime.now());
    } else {
      log.warn(
          "Missing [Enddate] for terminated [Encounter] {}, set default. Please check.",
          departmentCaseLogicId);
      newVisitDetail.setVisitDetailEndDate(LocalDate.now());
      newVisitDetail.setVisitDetailEndDatetime(LocalDateTime.now());
    }
  }

  /**
   * Delete FHIR Encounter resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param departmentCaseLogicId logical id of the FHIR Encounter resource
   * @param departmentCaseLogicIdentifier identifier of the FHIR Encounter resource
   */
  private void deleteExistingVisitDetails(
      String departmentCaseLogicId, String departmentCaseLogicIdentifier) {
    if (!Strings.isNullOrEmpty(departmentCaseLogicId)) {
      departmentCaseMapperService.deleteExistingDepartmentcaseByLogicalId(departmentCaseLogicId);
    } else {
      departmentCaseMapperService.deleteExistingDepartmentcaseByIdentifier(
          departmentCaseLogicIdentifier);
    }
  }
}
