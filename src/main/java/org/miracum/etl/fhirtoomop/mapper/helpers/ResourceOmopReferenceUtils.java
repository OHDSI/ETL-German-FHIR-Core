package org.miracum.etl.fhirtoomop.mapper.helpers;

import com.google.common.base.Strings;
import java.time.LocalDateTime;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.miracum.etl.fhirtoomop.repository.service.EncounterInstitutionContactMapperServiceImpl;
import org.miracum.etl.fhirtoomop.repository.service.PatientMapperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * The ResourceOmopReferenceUtils class is used for processing the information of referenced FHIR
 * resources and their representation in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@NoArgsConstructor
@Component
public class ResourceOmopReferenceUtils {

  private Boolean bulkload;
  private DbMappings dbMappings;
  private OmopRepository repositories;
  private Boolean dictionaryLoadInRam;
  @Autowired PatientMapperServiceImpl patientMapperService;
  @Autowired EncounterInstitutionContactMapperServiceImpl encounterMapperService;

  @Autowired
  @Qualifier("readerJdbcTemplate")
  JdbcTemplate readerTemplate;

  @Value("${data.fhirGateway.tableName}")
  private String inputTableName;

  /**
   * Constructor for objects of the class ResourceOmopReferenceUtils.
   *
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param bulkload flag to differentiate between bulk load or incremental load
   * @param dictionaryLoadInRam parameter which indicates whether referenced data is searched in RAM
   *     or in OMOP CDM database
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   * @param repositories OMOP CDM repositories
   * @param fhirSystem URL for NamingSystem of the FHIR resource
   */
  @Autowired
  public ResourceOmopReferenceUtils(
      Boolean bulkload,
      DbMappings dbMappings,
      OmopRepository repositories,
      Boolean dictionaryLoadInRam) {
    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
    this.repositories = repositories;
    this.dictionaryLoadInRam = dictionaryLoadInRam;
  }

  /**
   * Returns the person_id based on the logical id and/or identifier of the referenced FHIR Patient
   * resource from person table in OMOP CDM.
   *
   * @param patientReferenceIdentifier identifier of referenced FHIR Patient resource
   * @param patientReferenceLogicalId logical id of referenced FHIR Patient resource
   * @param resourceLogicalId logical id of processing FHIR resource
   * @return person_id in OMOP CDM
   */
  public Long getPersonId(
      String patientReferenceIdentifier,
      String patientReferenceLogicalId,
      String resourceLogicalId,
      String resourceId) {

    if (patientReferenceIdentifier == null && patientReferenceLogicalId == null) {
      log.warn("Unable to extract [Patient Reference] for {}. Skip resource", resourceId);
      return null;
    }

    if (bulkload.equals(Boolean.FALSE)) {
      return searchPersonInDb(
          patientReferenceIdentifier, patientReferenceLogicalId, resourceLogicalId);

    } else {
      if (dictionaryLoadInRam.equals(Boolean.TRUE)) {
        return searchPersonInRam(patientReferenceIdentifier, patientReferenceLogicalId);

      } else {
        return searchPersonInDb(
            patientReferenceIdentifier, patientReferenceLogicalId, resourceLogicalId);
      }
    }
  }

  /**
   * Searches the person_id based on the logical id and/or identifier of the referenced FHIR Patient
   * resource in RAM. For this purpose, all person records belonging to patients are loaded in RAM
   * for finding the person_id.
   *
   * @param patientReferenceIdentifier identifier of referenced FHIR Patient resource
   * @param patientReferenceLogicalId logical id of referenced FHIR Patient resource
   * @return person_id in OMOP CDM
   */
  private Long searchPersonInRam(
      String patientReferenceIdentifier, String patientReferenceLogicalId) {

    if (!Strings.isNullOrEmpty(patientReferenceLogicalId)) {
      var findPersonIdByLogicalId = dbMappings.getFindPersonIdByLogicalId();
      if (findPersonIdByLogicalId.containsKey(patientReferenceLogicalId)) {
        return findPersonIdByLogicalId.get(patientReferenceLogicalId);
      }
    }

    if (!Strings.isNullOrEmpty(patientReferenceIdentifier)) {
      var findPersonIdByIdentifier = dbMappings.getFindPersonIdByIdentifier();
      if (findPersonIdByIdentifier.containsKey(patientReferenceIdentifier)) {
        return findPersonIdByIdentifier.get(patientReferenceIdentifier);
      }
    }

    return null;
  }

  /**
   * Searches the person_id based on the logical id and/or identifier of the referenced FHIR Patient
   * resource directly in the OMOP CDM database.
   *
   * @param patientReferenceIdentifier identifier of referenced FHIR Patient resource
   * @param patientReferenceLogicalId logical id of referenced FHIR Patient resource
   * @param sourceId logical id of processing FHIR resource
   * @return person_id in OMOP CDM
   */
  private Long searchPersonInDb(
      String patientReferenceIdentifier, String patientReferenceLogicalId, String sourceId) {
    var existingPersonId =
        getExistingPersonId(patientReferenceIdentifier, patientReferenceLogicalId);

    //    if (existingPersonId == null && bulkload.equals(Boolean.FALSE)) {
    //      // TODO: use JPA instead of JDBCTemplate
    //      readerTemplate.update(
    //          String.format("UPDATE %s SET last_updated_at=? WHERE fhir_id=?", inputTableName),
    //          LocalDateTime.now().plusDays(1),
    //          sourceId.substring(4));
    //      log.warn(
    //          "No patient found for [{}]. This resource will be processed again tomorrow.",
    // sourceId);
    //      return null;
    //    }
    return existingPersonId;
  }

  /**
   * Searches if a FHIR Patient resource already exists in OMOP CDM.
   *
   * @param identifier identifier of the FHIR Patient resource
   * @param logicalId logical id of the FHIR Patient resource
   * @return person_id for the existing person
   */
  public Long getExistingPersonId(String identifier, String logicalId) {
    var existingPersonIdByLogicalId = getExistingPersonIdByLogicalId(logicalId);
    var existingPersonIdByIdentifier = getExistingPersonIdByIdentifier(identifier);
    if (existingPersonIdByLogicalId == null && existingPersonIdByIdentifier == null) {
      return null;
    } else if (existingPersonIdByLogicalId == null) {
      return existingPersonIdByIdentifier;
    } else {
      return existingPersonIdByLogicalId;
    }

    //    if (StringUtils.isNotBlank(logicalId)) {
    //      return getExistingPersonIdByLogicalId(logicalId);
    //    } else {
    //      return getExistingPersonIdByIdentifier(identifier);
    //    }
  }

  /**
   * Searches if a FHIR Patient resource already exists in OMOP CDM based on the identifier of the
   * FHIR Patient resource.
   *
   * @param identifier identifier of the FHIR Patient resource
   * @return person_id for the existing person
   */
  private Long getExistingPersonIdByIdentifier(String identifier) {
    if (StringUtils.isBlank(identifier)) {
      return null;
    }
    return patientMapperService.findPersonIdByFhirIdentifier(identifier);
  }

  /**
   * Searches if a FHIR Patient resource already exists in OMOP CDM based on the logical id of the
   * FHIR Patient resource.
   *
   * @param logicalId logical id of the FHIR Patient resource
   * @return person_id for the existing person
   */
  private Long getExistingPersonIdByLogicalId(String logicalId) {
    if (StringUtils.isBlank(logicalId)) {
      return null;
    }
    return patientMapperService.findPersonIdByFhirLogicalId(logicalId);
  }

  /**
   * Returns the visit_occurrence_id based on the logical id and/or identifier of the referenced
   * FHIR Encounter resource from visit_occurrence table in OMOP CDM.
   *
   * @param encounterReferenceIdentifier identifier of referenced FHIR Encounter resource
   * @param encounterReferenceLogicalId logical id of referenced FHIR Encounter resource
   * @param personId referenced person_id
   * @param sourceId logical id of processing FHIR resource
   * @return visit_occurrence_id in OMOP CDM
   */
  public Long getVisitOccId(
      String encounterReferenceIdentifier,
      String encounterReferenceLogicalId,
      Long personId,
      String resourceId) {

    if (encounterReferenceIdentifier == null && encounterReferenceLogicalId == null) {
      log.warn("Unable to extract [Encounter Reference] for {}.", resourceId);
      return null;
    }
    if (bulkload.equals(Boolean.FALSE)) {
      return searchVisitOccInDb(
          encounterReferenceIdentifier, encounterReferenceLogicalId, resourceId);

    } else {
      if (dictionaryLoadInRam.equals(Boolean.TRUE)) {
        return searchVisitOccInRam(encounterReferenceIdentifier, encounterReferenceLogicalId);
      } else {
        return searchVisitOccInDb(
            encounterReferenceIdentifier, encounterReferenceLogicalId, resourceId);
      }
    }
  }

  /**
   * Searches the visit_occurrence_id based on the logical id and/or identifier of the referenced
   * FHIR Encounter resource in RAM. For this purpose, all visit_occurrence records belonging to
   * encounters are loaded in RAM for finding the visit_occurrence_id.
   *
   * @param encounterReferenceIdentifier identifier of referenced FHIR Encounter resource
   * @param encounterReferenceLogicalId logical id of referenced FHIR Encounter resource
   * @return visit_occurrence_id in OMOP CDM
   */
  private Long searchVisitOccInRam(
      String encounterReferenceIdentifier, String encounterReferenceLogicalId) {

    if (!Strings.isNullOrEmpty(encounterReferenceLogicalId)) {
      var findVisitOccIdByLogicalId = dbMappings.getFindVisitOccIdByLogicalId();
      if (findVisitOccIdByLogicalId.containsKey(encounterReferenceLogicalId)) {
        return findVisitOccIdByLogicalId.get(encounterReferenceLogicalId);
      }
    }

    if (!Strings.isNullOrEmpty(encounterReferenceIdentifier)) {
      var findVisitOccIdByIdentifier = dbMappings.getFindVisitOccIdByIdentifier();
      if (findVisitOccIdByIdentifier.containsKey(encounterReferenceIdentifier)) {
        return findVisitOccIdByIdentifier.get(encounterReferenceIdentifier);
      }
    }

    return null;
  }

  /**
   * Searches the visit_occurrence_id based on the logical id and/or identifier of the referenced
   * FHIR Encounter resource directly in the OMOP CDM database.
   *
   * @param encounterReferenceIdentifier identifier of referenced FHIR Encounter resource
   * @param encounterReferenceLogicalId logical id of referenced FHIR Encounter resource
   * @param sourceId logical id of processing FHIR resource
   * @return visit_occurrence_id in OMOP CDM
   */
  private Long searchVisitOccInDb(
      String encounterReferenceIdentifier, String encounterReferenceLogicalId, String resourceId) {
    var existingVisitOccId =
        getExistingVisitOccId(encounterReferenceLogicalId, encounterReferenceIdentifier);
    if (existingVisitOccId == null && bulkload.equals(Boolean.FALSE)) {
      // TODO: use JPA instead of JDBCTemplate
      readerTemplate.update(
          String.format("UPDATE %s SET last_updated_at=? WHERE fhir_id=?", inputTableName),
          LocalDateTime.now().plusDays(1),
          resourceId.substring(4));
      log.warn(
          "No [visit_occurrence] found for {}. This resource will be processed again tomorrow.",
          resourceId);
      return null;
    }

    return existingVisitOccId;
  }

  /**
   * Searches if a FHIR Encounter resource already exists in OMOP CDM.
   *
   * @param logicalId logical id of the FHIR Encounter resource
   * @param identifier identifier of the FHIR Encounter resource
   * @return visit_occurrence_id for existing visit_occurrence
   */
  public Long getExistingVisitOccId(String logicalId, String identifier) {
    var existingVisitOccByLogicalId = getExistingVisitOccIdByLogicalId(logicalId);
    var existingVisitOccByIdentifier = getExistingVisitOccIdByIdentifier(identifier);
    if (existingVisitOccByLogicalId == null && existingVisitOccByIdentifier == null) {
      return null;
    } else if (existingVisitOccByLogicalId == null) {
      return existingVisitOccByIdentifier;
    } else {
      return existingVisitOccByLogicalId;
    }
    //    if (!Strings.isNullOrEmpty(logicalId)) {
    //      return getExistingVisitOccIdByLogicalId(logicalId);
    //    } else {
    //      return getExistingVisitOccIdByIdentifier(identifier);
    //    }
  }

  /**
   * Searches if a FHIR Encounter resource already exists in OMOP CDM based on the identifier of the
   * FHIR Encounter resource.
   *
   * @param identifier identifier of the FHIR Encounter resource
   * @return visit_occurrence_id for the existing visit_occurrence
   */
  private Long getExistingVisitOccIdByIdentifier(String identifier) {
    if (StringUtils.isBlank(identifier)) {
      return null;
    }
    return encounterMapperService.findVisitsOccIdByFhirIdentifier(identifier);
  }

  /**
   * Searches if a FHIR Encounter resource already exists in OMOP CDM based on the logical id of the
   * FHIR Encounter resource.
   *
   * @param logicalId logical id of the FHIR Encounter resource
   * @return visit_occurrence_id for the existing visit_occurrence
   */
  private Long getExistingVisitOccIdByLogicalId(String logicalId) {
    if (StringUtils.isBlank(logicalId)) {
      return null;
    }
    return encounterMapperService.findVisitsOccIdByFhirLogicalId(logicalId);
  }

  /**
   * Creates a new record of the medication_id_map table in OMOP CDM.
   *
   * @param type resource type of the FHIR Medication resource
   * @param logicId logical id of the FHIR Medication resource
   * @param sourceIdentifier identifier of the FHIR Medication resource
   * @param atc ATC code from the FHIR Medication resource
   * @return record of the medication_id_map table
   */
  public MedicationIdMap createMedicationIdMap(
      String type, String logicId, String sourceIdentifier, String atc) {

    return MedicationIdMap.builder()
        .type(type)
        .fhirLogicalId(logicId)
        .fhirIdentifier(sourceIdentifier)
        .atc(atc)
        .build();
  }
}
