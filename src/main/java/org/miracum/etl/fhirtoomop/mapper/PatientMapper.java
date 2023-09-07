package org.miracum.etl.fhirtoomop.mapper;

import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_EHR_RECORD_STATUS_DECEASED;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_GENDER_UNKNOWN;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_HISPANIC_OR_LATINO;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_NO_MATCHING_CONCEPT;
import static org.miracum.etl.fhirtoomop.Constants.CONCEPT_UNKNOWN_RACIAL_GROUP;
import static org.miracum.etl.fhirtoomop.Constants.ETHNICITY_SOURCE_HISPANIC_OR_LATINO;
import static org.miracum.etl.fhirtoomop.Constants.ETHNICITY_SOURCE_MIXED;
import static org.miracum.etl.fhirtoomop.Constants.MAX_LOCATION_CITY_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.MAX_LOCATION_COUNTRY_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.MAX_LOCATION_STATE_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.MAX_LOCATION_ZIP_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.MAX_SOURCE_VALUE_LENGTH;
import static org.miracum.etl.fhirtoomop.Constants.SOURCE_VOCABULARY_ID_GENDER;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.r4.model.Address;
import org.hl7.fhir.r4.model.Age;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Patient;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.miracum.etl.fhirtoomop.model.omop.Person;
import org.miracum.etl.fhirtoomop.repository.service.PatientMapperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The PatientMapper class describes the business logic of transforming a FHIR Patient resource to
 * OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class PatientMapper implements FhirMapper<Patient> {
  private static final FhirSystems fhirSystems = new FhirSystems();
  private final IFhirPath fhirPath;
  private final Boolean bulkload;
  private final DbMappings dbMappings;

  @Autowired ResourceOmopReferenceUtils omopReferenceUtils;
  @Autowired ResourceFhirReferenceUtils fhirReferenceUtils;
  @Autowired PatientMapperServiceImpl patientService;
  @Autowired ResourceCheckDataAbsentReason checkDataAbsentReason;
  @Autowired FindOmopConcepts findOmopConcepts;

  private static final Counter noFhirReferenceCounter =
      MapperMetrics.setNoFhirReferenceCounter("stepProcessPatients");
  private static final Counter deletedFhirReferenceCounter =
      MapperMetrics.setDeletedFhirRessourceCounter("stepProcessPatients");

  /**
   * Constructor for objects of the class PatientMapper.
   *
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param bulkload parameter which indicates whether the Job should be run as bulk load or
   *     incremental load
   * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
   */
  @Autowired
  public PatientMapper(IFhirPath fhirPath, Boolean bulkload, DbMappings dbMappings) {

    this.fhirPath = fhirPath;
    this.bulkload = bulkload;
    this.dbMappings = dbMappings;
  }

  /**
   * Maps a FHIR Patient resource to several OMOP CDM tables.
   *
   * @param srcPatient FHIR Patient resource
   * @param isDeleted a flag, whether the FHIR resource is deleted in the source
   * @return OmopModelWrapper cache of newly created OMOP CDM records from the FHIR Patient resource
   */
  @Override
  public OmopModelWrapper map(Patient srcPatient, boolean isDeleted) {

    var wrapper = new OmopModelWrapper();

    var patientSourceIdentifier = fhirReferenceUtils.extractIdentifier(srcPatient, "MR");
    var patientLogicId = fhirReferenceUtils.extractId(srcPatient);
    if (Strings.isNullOrEmpty(patientLogicId) && Strings.isNullOrEmpty(patientSourceIdentifier)) {
      log.warn("No [Identifier] or [Id] found. [Patient] resource is invalid. Skip resource");
      noFhirReferenceCounter.increment();
      return null;
    }

    String patientId = "";
    if (!Strings.isNullOrEmpty(patientLogicId)) {
      patientId = srcPatient.getId();
    }

    var ageExtensionMap = extractAgeExtension(srcPatient);
    var ageAtDiagnosis = setAgeAtDiagnosis(patientLogicId, patientSourceIdentifier);
    var realBirthDate = extractBirthDate(srcPatient);
    var calculatedBirthDate =
        extractCalculatedBirthDate(ageExtensionMap, ageAtDiagnosis, patientId);

    if (realBirthDate == null && calculatedBirthDate == null) {
      log.info("No [Birthdate] found for [Patient]: {}. Skip Resource.", patientId);
      if (bulkload.equals(Boolean.FALSE)) {
        deleteExistingPatients(patientLogicId, patientSourceIdentifier);
      }
      return null;
    }

    if (bulkload.equals(Boolean.FALSE) && isDeleted) {
      log.info("Found a deleted [Patient] resource {}. Deleting from OMOP DB.", patientId);
      deleteExistingPatients(patientLogicId, patientSourceIdentifier);
      deletedFhirReferenceCounter.increment();
      return null;
    }

    if (bulkload.equals(Boolean.FALSE)) {
      deleteExistingDeath(patientLogicId, patientSourceIdentifier);
      deleteExistingCalculatedBirthYear(patientLogicId, patientSourceIdentifier);
    }

    var newPerson = createNewPerson(srcPatient, patientLogicId, patientSourceIdentifier, patientId);
    setBirthDate(realBirthDate, calculatedBirthDate, newPerson);

    var ethnicGroupCoding = extractEthnicGroup(srcPatient);
    setRaceConcept(ethnicGroupCoding, newPerson, patientLogicId);
    setEthnicityConcept(ethnicGroupCoding, newPerson);

    var death = setDeath(srcPatient, patientLogicId, patientSourceIdentifier);
    if (death != null) {
      wrapper.getPostProcessMap().add(death);
    }

    var location = setLocation(srcPatient, patientLogicId, patientSourceIdentifier);
    if (location != null) {
      wrapper.getPostProcessMap().add(location);
    }

    wrapper.setPerson(newPerson);

    if (ageAtDiagnosis.getDataOne() != null) {
      wrapper.getPostProcessMap().add(ageAtDiagnosis);
    }
    return wrapper;
  }

  private PostProcessMap setAgeAtDiagnosis(String patientLogicId, String patientSourceIdentifier) {
    return PostProcessMap.builder()
        .type(ResourceType.PATIENT.name())
        .omopTable("age_at_diagnosis")
        .omopId(Long.valueOf(4307859))
        .fhirIdentifier(patientSourceIdentifier)
        .fhirLogicalId(patientLogicId)
        .build();
  }

  /**
   * Delete FHIR Patient resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param patientLogicId logical id of the FHIR Patient resource
   * @param patientSourceIdentifier identifier of the FHIR Patient resource
   */
  private void deleteExistingPatients(String patientLogicId, String patientSourceIdentifier) {
    if (!Strings.isNullOrEmpty(patientLogicId)) {
      patientService.deletePersonByFhirLogicalId(patientLogicId);
    } else {
      patientService.deletePersonByFhirIdentifier(patientSourceIdentifier);
    }
  }

  /**
   * Creates a new record of the person table in OMOP CDM for the processed FHIR Patient resource.
   *
   * @param srcPatient FHIR Patient resource
   * @param patientLogicId logical id of the FHIR Patient resource
   * @param patientSourceIdentifier identifier of the FHIR Patient resource
   * @return new record of the person table in OMOP CDM for the processed FHIR Patient resource
   */
  private Person createNewPerson(
      Patient srcPatient, String patientLogicId, String patientSourceIdentifier, String patientId) {
    var personSourceValue = cutString(patientSourceIdentifier, MAX_SOURCE_VALUE_LENGTH);

    var person =
        Person.builder()
            .personSourceValue(personSourceValue == null ? null : personSourceValue.substring(4))
            .fhirLogicalId(patientLogicId)
            .fhirIdentifier(patientSourceIdentifier)
            .build();
    var gender = getGender(srcPatient);
    person.setGenderConceptId(getGenderConceptId(gender));
    person.setGenderSourceValue(gender);

    if (bulkload.equals(Boolean.FALSE)) {
      var existingPersonId =
          omopReferenceUtils.getExistingPersonId(patientSourceIdentifier, patientLogicId);
      if (existingPersonId != null) {
        log.debug("[Patient] {} exists already in person. Update existing person", patientId);

        person.setPersonId(existingPersonId);
      }
    }
    return person;
  }

  /**
   * Set the information for ethnicity in person record.
   *
   * @param ethnicGroupCoding coding of ethnic group
   * @param person new record of the person table in OMOP CDM
   */
  private void setEthnicityConcept(Coding ethnicGroupCoding, Person person) {
    if (ethnicGroupCoding == null || Strings.isNullOrEmpty(ethnicGroupCoding.getCode())) {
      person.setEthnicityConceptId(CONCEPT_NO_MATCHING_CONCEPT);
      return;
    }

    var ethnicGroupCode = ethnicGroupCoding.getCode();
    if (ethnicGroupCode.equals(ETHNICITY_SOURCE_HISPANIC_OR_LATINO)) {
      person.setEthnicityConceptId(CONCEPT_HISPANIC_OR_LATINO);
      person.setEthnicitySourceConceptId(CONCEPT_HISPANIC_OR_LATINO);
      person.setEthnicitySourceValue(ETHNICITY_SOURCE_HISPANIC_OR_LATINO);

    } else if (ethnicGroupCode.equals(ETHNICITY_SOURCE_MIXED)) {
      person.setEthnicityConceptId(CONCEPT_NO_MATCHING_CONCEPT);
      person.setEthnicitySourceValue(ethnicGroupCode);
    } else {
      person.setEthnicityConceptId(CONCEPT_NO_MATCHING_CONCEPT);
    }
  }

  /**
   * Set the information for race in person record.
   *
   * @param ethnicGroupCoding coding of ethnic group
   * @param person new record of the person table in OMOP CDM
   */
  private void setRaceConcept(Coding ethnicGroupCoding, Person person, String patientLogicId) {

    if (ethnicGroupCoding == null || Strings.isNullOrEmpty(ethnicGroupCoding.getCode())) {
      person.setRaceConceptId(CONCEPT_UNKNOWN_RACIAL_GROUP);
      return;
    }
    var ethnicGroupCode = ethnicGroupCoding.getCode();
    if (ethnicGroupCode.equals(ETHNICITY_SOURCE_HISPANIC_OR_LATINO)) {
      person.setRaceConceptId(CONCEPT_UNKNOWN_RACIAL_GROUP);
    } else if (ethnicGroupCode.equals(ETHNICITY_SOURCE_MIXED)) {
      person.setRaceConceptId(CONCEPT_NO_MATCHING_CONCEPT);
      person.setRaceSourceValue(ethnicGroupCode);
    } else {
      var ethnicConcept =
          findOmopConcepts.getSnomedRaceConcepts(
              ethnicGroupCoding, bulkload, dbMappings, patientLogicId);
      if (ethnicConcept != null) {
        person.setRaceConceptId(ethnicConcept.getStandardRaceConceptId());
        person.setRaceSourceConceptId(ethnicConcept.getSnomedConceptId());
        person.setRaceSourceValue(ethnicGroupCode);
      }
    }
  }

  /**
   * Creates a new record of the post_process_map table in OMOP CDM for extracted location
   * informations from the processed FHIR Patient resource.
   *
   * @param srcPatient FHIR Patient resource
   * @param patientLogicId logical id of the FHIR Patient resource
   * @param patientSourceIdentifier identifier of the FHIR Patient resource
   * @return new record of the post_process_map table in OMOP CDM for extracted location
   *     informations from the processed FHIR Patient resource
   */
  private PostProcessMap setLocation(
      Patient srcPatient, String patientLogicId, String patientSourceIdentifier) {
    if (!srcPatient.hasAddress() || srcPatient.getAddress().isEmpty()) {
      return null;
    }

    var address = srcPatient.getAddressFirstRep();
    if (address.getExtensionByUrl(fhirSystems.getDataAbsentReason()) != null) {
      return null;
    }
    StringBuilder dataOne = new StringBuilder();
    StringBuilder dataTwo = new StringBuilder();

    addZip(address, dataOne);
    dataOne.append(";");
    addCity(address, dataOne);
    dataOne.append(";");
    addCountry(address, dataOne);

    addLines(address, dataTwo);
    dataTwo.append(";");
    addState(address, dataTwo);

    var dataOneStr = dataOne.toString();
    var dataTwoStr = dataTwo.toString();
    //    if (Strings.isNullOrEmpty(dataOneStr) && Strings.isNullOrEmpty(dataTwoStr)) {
    //      return null;
    //    }

    if (dataOneStr.equals(";;") && dataTwoStr.equals(";")) {
      return null;
    }

    return PostProcessMap.builder()
        .dataOne(dataOne.toString())
        .dataTwo(dataTwo.toString())
        .omopTable(OmopModelWrapper.Tablename.LOCATION.name())
        .type(ResourceType.PATIENT.name())
        .fhirLogicalId(patientLogicId)
        .fhirIdentifier(patientSourceIdentifier)
        .build();
  }

  /**
   * Append the state information (if exists) to column dataTwo from POST_PROCESS_MAP table.
   *
   * @param address Address component from Patient FHIR resource
   * @param dataTwo StringBuilder for the dataTwo column from POST_PROCESS_MAP table
   */
  private void addState(Address address, StringBuilder dataTwo) {
    var stateElement = address.getStateElement();
    if (stateElement.isEmpty()) {
      return;
    }
    var state = checkDataAbsentReason.getValue(stateElement);

    if (!Strings.isNullOrEmpty(state)) {
      dataTwo.append(cutString(state, MAX_LOCATION_STATE_LENGTH));
    }
  }

  /**
   * Append the street information (if exists) to column dataTwo from POST_PROCESS_MAP table.
   *
   * @param address Address component from Patient FHIR resource
   * @param dataTwo StringBuilder for the dataTwo column from POST_PROCESS_MAP table
   */
  private void addLines(Address address, StringBuilder dataTwo) {
    var lines = address.getLine();
    if (lines.isEmpty()) {
      return;
    }

    for (var line : lines) {
      if (line.isEmpty()) {
        continue;
      }
      var lineStr = checkDataAbsentReason.getValue(line);
      if (!Strings.isNullOrEmpty(lineStr)) {
        dataTwo.append(line);
        dataTwo.append(" ");
      }
    }
    //    if (!dataTwo.isEmpty()) {
    //      dataTwo.append(";");
    //    }
  }

  /**
   * Append the country information (if exists) to column dataTwo from POST_PROCESS_MAP table.
   *
   * @param address Address component from Patient FHIR resource
   * @param dataOne StringBuilder for the dataOne column from POST_PROCESS_MAP table
   */
  private void addCountry(Address address, StringBuilder dataOne) {
    var countryElement = address.getCountryElement();
    if (countryElement.isEmpty()) {
      return;
    }
    var country = checkDataAbsentReason.getValue(countryElement);
    if (!Strings.isNullOrEmpty(country)) {
      dataOne.append(cutString(country.replaceAll("\\s+", ""), MAX_LOCATION_COUNTRY_LENGTH));
    }
  }

  /**
   * Append the city information (if exists) to column dataTwo from POST_PROCESS_MAP table.
   *
   * @param address Address component from Patient FHIR resource
   * @param dataOne StringBuilder for the dataOne column from POST_PROCESS_MAP table
   */
  private void addCity(Address address, StringBuilder dataOne) {
    var cityElement = address.getCityElement();
    if (cityElement.isEmpty()) {
      return;
    }
    var city = checkDataAbsentReason.getValue(cityElement);
    if (!Strings.isNullOrEmpty(city)) {
      dataOne.append(cutString(city, MAX_LOCATION_CITY_LENGTH));
    }
  }

  /**
   * Append the zip code information (if exists) to column dataTwo from POST_PROCESS_MAP table.
   *
   * @param address Address component from Patient FHIR resource
   * @param dataOne StringBuilder for the dataOne column from POST_PROCESS_MAP table
   */
  private void addZip(Address address, StringBuilder dataOne) {
    var zipElement = address.getPostalCodeElement();
    if (zipElement.isEmpty()) {
      return;
    }
    var zip = checkDataAbsentReason.getValue(zipElement);
    if (!Strings.isNullOrEmpty(zip)) {
      dataOne.append(cutString(zip, MAX_LOCATION_ZIP_LENGTH));
    }
  }

  /**
   * Shortens a string value to a specified maximum length.
   *
   * @param stringToBeCut string value to be shortened
   * @param maxLength maximum length of the string value
   * @return shortened string value
   */
  private String cutString(String stringToBeCut, int maxLength) {
    if (!Strings.isNullOrEmpty(stringToBeCut) && stringToBeCut.length() > maxLength) {
      log.debug(
          "The String: {} is longer than allowed. Cut it to a length of {}.",
          stringToBeCut,
          maxLength);
      return StringUtils.left(stringToBeCut, maxLength);
    }
    return stringToBeCut;
  }

  /**
   * Sets the extracted birth date information from FHIR Patient resource to the new person record.
   *
   * @param birthDate the birth date in dateType from FHIR Patient resource
   * @param person record of the person table in OMOP CDM for the processed FHIR Patient resource
   */
  private void setBirthDate(
      LocalDateTime realBirthDate, LocalDateTime calculatedBirthDate, Person person) {
    if (realBirthDate != null) {
      person.setYearOfBirth(realBirthDate.getYear());
      person.setMonthOfBirth(realBirthDate.getMonthValue());
      person.setDayOfBirth(realBirthDate.getDayOfMonth());
    } else {
      person.setYearOfBirth(calculatedBirthDate.getYear());
    }
  }

  /**
   * Extracts birth date information from FHIR Patient resource.
   *
   * @param srcPatient FHIR Patient resource
   * @param patientLogicId logical id of the FHIR Patient resource
   * @return birth date or the calculated birth date from FHIR Patient resource
   */
  private LocalDateTime extractBirthDate(Patient srcPatient) {
    var birthDate = fhirPath.evaluateFirst(srcPatient, "Patient.birthDate", DateType.class);
    if (birthDate.isPresent()) {
      var birthDateElement = birthDate.get();
      if (!birthDateElement.hasValue() || birthDateElement.getValue() == null) {
        return null;
      }
      return LocalDateTime.ofInstant(
          birthDateElement.getValue().toInstant(), ZoneId.of("Europe/Berlin"));
    } else {
      return null;
    }
  }

  /**
   * Calculate birth date from FHIR Patient resource.
   *
   * @param srcPatient FHIR Patient resource
   * @return calculated birth date from FHIR Patient resource
   */
  private LocalDateTime extractCalculatedBirthDate(
      Map<String, Extension> ageExtensionMap, PostProcessMap ageAtDiagnosis, String patientId) {
    if (ageExtensionMap.isEmpty()) {
      return null;
    }

    Age ageValue = extractAgeValue(ageExtensionMap);
    var documentationDateTime = extractDocumentationDateTime(ageExtensionMap);
    if (ageValue == null || documentationDateTime == null || ageValue.getCode() == null) {
      return null;
    }

    var ageUnit = ageValue.getCode();
    var age = ageValue.getValue().intValue();

    ageAtDiagnosis.setDataOne(documentationDateTime.toString());
    ageAtDiagnosis.setDataTwo(age + ":" + ageValue.getUnit() + ":" + ageValue.getCode());

    switch (ageUnit) {
      case "a":
        return documentationDateTime.minusYears(age);
      case "mo":
        return documentationDateTime.minusMonths(age);

      case "d":
        return documentationDateTime.minusDays(age);
      default:
        log.warn("Unable to calculate [Birthdate] for [Patient]: {}.", patientId);
        return null;
    }
  }

  /**
   * @param srcPatient
   * @return
   */
  private Map<String, Extension> extractAgeExtension(Patient srcPatient) {
    var ageExtension = srcPatient.getExtensionByUrl(fhirSystems.getAgeExtension());
    if (ageExtension == null) {
      return Collections.emptyMap();
    }

    var subExtensions = ageExtension.getExtension();

    return subExtensions.stream().collect(Collectors.toMap(Extension::getUrl, v -> v));
  }

  /**
   * @param ageExtensionMap
   * @return
   */
  private Age extractAgeValue(Map<String, Extension> ageExtensionMap) {
    if (ageExtensionMap.isEmpty()) {
      return null;
    }
    var ageExtensionAge = ageExtensionMap.get("age");
    if (ageExtensionAge == null) {

      return null;
    }
    Age ageValue = (Age) ageExtensionAge.getValue();
    if (ageValue == null) {
      return null;
    }
    return ageValue;
  }

  /**
   * Extract the date of documentation from a FHIR Patient resource
   *
   * @param ageExtensionMap the extensions from ageExtention element in a FHIR Patient resource as a
   *     Map.
   * @return the date of documentation
   */
  private LocalDateTime extractDocumentationDateTime(Map<String, Extension> ageExtensionMap) {
    if (ageExtensionMap.isEmpty()) {
      return null;
    }
    var documentationDateTimeExtension = ageExtensionMap.get("dateTimeOfDocumentation");
    if (documentationDateTimeExtension == null) {
      return null;
    }
    var extensionValue = (DateTimeType) documentationDateTimeExtension.getValue();
    if (extensionValue == null) {
      return null;
    }
    return extensionValue
        .getValue()
        .toInstant()
        .atZone(ZoneId.of("Europe/Berlin"))
        .toLocalDateTime();
  }

  /**
   * Extract coding of ethnic group from a FHIR Patient resource
   *
   * @param srcPatient FHIR Patient resource
   * @return coding of ethnic group
   */
  private Coding extractEthnicGroup(Patient srcPatient) {
    var ethnicGroupExtension = srcPatient.getExtensionByUrl(fhirSystems.getEthnicGroupExtension());
    if (ethnicGroupExtension == null) {
      return null;
    }
    return ethnicGroupExtension.getValue().castToCoding(ethnicGroupExtension.getValue());
  }

  /**
   * Extracts gender information from FHIR Patient resource.
   *
   * @param srcPatient FHIR Patient resource
   * @return the gender from FHIR Patient resource
   */
  private String getGender(Patient srcPatient) {
    var genderElement = srcPatient.getGenderElement();
    if (genderElement.isEmpty()) {
      return null;
    }
    var gender = checkDataAbsentReason.getValue(genderElement);
    if (Strings.isNullOrEmpty(gender)) {
      return null;
    }

    if (gender.equals("other")
        && genderElement.hasExtension(fhirSystems.getGenderAmtlichDeExtension())) {
      var administrativeGenderType =
          genderElement.getExtensionByUrl(fhirSystems.getGenderAmtlichDeExtension()).getValue();
      var administrativeGender = administrativeGenderType.castToCoding(administrativeGenderType);
      if (administrativeGender.hasCode()) {
        return administrativeGender.getCode();
      }
    }
    return gender;
  }
  /**
   * Mapping gender information from FHIR Patient resource to OMOP Concept.
   *
   * @param gender gender as String from FHIR Patient resource
   * @return the gender_concept_id of the gender from FHIR Patient resource
   */
  private Integer getGenderConceptId(String gender) {
    if (StringUtils.isBlank(gender)) {
      return CONCEPT_GENDER_UNKNOWN;
    }
    var sourceToConcepMap =
        findOmopConcepts.getCustomConcepts(gender, SOURCE_VOCABULARY_ID_GENDER, dbMappings);
    return sourceToConcepMap.getTargetConceptId();
  }

  /**
   * Extracts death information from the processed FHIR Patient resource and creates a new record of
   * the post_process_map table in OMOP CDM.
   *
   * @param srcPatient FHIR Patient resource
   * @param patientLogicId logical id of the FHIR Patient resource
   * @param patientSourceIdentifier identifier of the FHIR Patient resource
   * @return new record of the post_process_map table in OMOP CDM for death data from the processed
   *     FHIR Patient resource
   */
  private PostProcessMap setDeath(
      Patient srcPatient, String patientLogicId, String patientSourceIdentifier) {

    if (!srcPatient.hasDeceasedDateTimeType()
        || srcPatient.getDeceasedDateTimeType() == null
        || !srcPatient.getDeceasedDateTimeType().hasValue()
            && srcPatient.getDeceasedDateTimeType().getValue() == null) {
      return null;
    }

    var deathDateTime =
        new Timestamp(srcPatient.getDeceasedDateTimeType().getValue().getTime()).toLocalDateTime();

    return PostProcessMap.builder()
        .dataOne(deathDateTime.toLocalDate().toString())
        .dataTwo(deathDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
        .type(ResourceType.PATIENT.name())
        .omopId(Long.valueOf(CONCEPT_EHR_RECORD_STATUS_DECEASED))
        .omopTable(OmopModelWrapper.Tablename.DEATH.getTableName())
        .fhirLogicalId(patientLogicId)
        .fhirIdentifier(patientSourceIdentifier)
        .build();
  }

  /**
   * Delete FHIR Patient resources from OMOP CDM tables using fhir_logical_id and fhir_identifier
   *
   * @param patientLogicId logical id of the FHIR Patient resource
   * @param patientSourceIdentifier identifier of the FHIR Patient resource
   */
  private void deleteExistingDeath(String patientLogicId, String patientSourceIdentifier) {
    if (!Strings.isNullOrEmpty(patientLogicId)) {
      patientService.deleteExistingDeathByFhirLogicalId(patientLogicId);
    } else {
      patientService.deleteExistingDeathByFhirIdentifier(patientSourceIdentifier);
    }
  }

  private void deleteExistingCalculatedBirthYear(
      String patientLogicId, String patientSourceIdentifier) {
    if (!Strings.isNullOrEmpty(patientLogicId)) {
      patientService.deleteExistingCalculatedBirthYearByFhirLogicalId(patientLogicId);
    } else {
      patientService.deleteExistingCalculatedBirthYearByFhirIdentifier(patientSourceIdentifier);
    }
  }
}
