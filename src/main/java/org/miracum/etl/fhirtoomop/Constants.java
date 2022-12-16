package org.miracum.etl.fhirtoomop;

import com.google.common.collect.ImmutableList;
import java.time.LocalDate;

/**
 * The class Constants contains all static values which are needed for the mapping from FHIR to OMOP
 * CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public class Constants {
  public static final int MAX_SOURCE_VALUE_LENGTH = 50;
  public static final int MAX_LOCATION_ZIP_LENGTH = 9;
  public static final int MAX_LOCATION_COUNTRY_LENGTH = 2;
  public static final int MAX_LOCATION_STATE_LENGTH = 20;
  public static final int MAX_LOCATION_CITY_LENGTH = 50;

  public static final int CONCEPT_NO_MATCHING_CONCEPT = 0;
  public static final int CONCEPT_GENDER_UNKNOWN = 4214687;
  public static final int CONCEPT_UNKNOWN_RACIAL_GROUP = 4218674;
  public static final int CONCEPT_EHR_RECORD_STATUS_DECEASED = 38003569;
  public static final int CONCEPT_PRIMARY_DIAGNOSIS = 32902;
  public static final int CONCEPT_SECONDARY_DIAGNOSIS = 32908;
  public static final int CONCEPT_EHR = 32817;
  public static final int CONCEPT_STILL_PATIENT = 32220;
  public static final int CONCEPT_INPATIENT = 9201;
  public static final int CONCEPT_HISPANIC_OR_LATINO = 38003563;
  public static final int CONCEPT_RESUSCITATION_STATUS = 4127294;
  public static final int CONCEPT_STAGE = 4106767;
  public static final int CONCEPT_SEVERITY = 4077563;

  public static final String ETHNICITY_SOURCE_HISPANIC_OR_LATINO = "2135-2";
  public static final String ETHNICITY_SOURCE_MIXED = "26242008";

  public static final String STAR_CROSS_CODING_REGEX = "[+†*!]*";

  public static final String VOCABULARY_ICD10GM = "ICD10GM";
  public static final String VOCABULARY_UCUM = "UCUM";
  public static final String VOCABULARY_LOINC = "LOINC";
  public static final String VOCABULARY_ATC = "ATC";
  public static final String VOCABULARY_OPS = "OPS";
  public static final String VOCABULARY_SNOMED = "SNOMED";
  public static final String VOCABULARY_DOMAIN = "Domain";

  public static final String SOURCE_VOCABULARY_ID_GENDER = "Gender";
  public static final String SOURCE_VOCABULARY_ID_PROCEDURE_BODYSITE = "Procedure Bodysite";
  public static final String SOURCE_VOCABULARY_ID_VISIT_TYPE = "Visit Type";
  public static final String SOURCE_VOCABULARY_ID_VISIT_STATUS = "Visit Status";
  public static final String SOURCE_VOCABULARY_ID_VISIT_DETAIL_STATUS = "Visit Detail Status";
  public static final String SOURCE_VOCABULARY_ID_LAB_RESULT = "Lab Result";
  public static final String SOURCE_VOCABULARY_ID_LAB_INTERPRETATION = "Lab Interpretation";
  public static final String SOURCE_VOCABULARY_ID_OBSERVATION_CATEGORY = "Observation Category";
  public static final String SOURCE_VOCABULARY_ID_DIAGNOSTIC_CONFIDENCE = "Diagnostic Conf.";
  public static final String SOURCE_VOCABULARY_ID_ICD_LOCALIZATION = "ICD Localization";
  public static final String SOURCE_VOCABULARY_ID_PROCEDURE_DICOM = "Procedure DICOM";
  public static final String SOURCE_VOCABULARY_ID_DIAGNOSIS_TYPE = "Diagnosis Type";

  public static final String SOURCE_VOCABULARY_ID_DIAGNOSTIC_REPORT_CATEGORY = "Diag.Rep Category";
  public static final String SOURCE_VOCABULARY_ID_ECRF_PARAMETER = "ECRF Parameter";
  public static final String SOURCE_VOCABULARY_ROUTE = "EDQM";
  public static final String SOURCE_VOCABULARY_SOFA_CATEGORY = "SOFA category";
  public static final String SOURCE_VOCABULARY_FRAILTY_SCORE = "Frailty score";

  public static final String FETCH_RESOURCES_LOG =
      "==== Fetching [{}] resources from source database ====";
  public static final String PROCESSING_RESOURCES_LOG = "Processing {} with id: {}";

  public static final LocalDate DEFAULT_BEGIN_DATE = LocalDate.parse("1800-01-01");
  public static final LocalDate DEFAULT_END_DATE = LocalDate.parse("2099-12-31");

  public static final int CONCEPT_EHR_MEDICATION_LIST = 32830;
  public static final int CONCEPT_CLAIM = 32810;
  public static final int CONCEPT_FOR_RESUSCITATION = 4126324;
  public static final int CONCEPT_NOT_FOR_RESUSCITATION = 4119499;
  public static final String SNOMED_FOR_RESUSCITATION = "304252001";
  public static final String SNOMED_NOT_FOR_RESUSCITATION = "304253006";

  public static final String FHIR_RESOURCE_OBSERVATION = "Observation";
  public static final String FHIR_RESOURCE_CONDITION = "Condition";
  public static final String FHIR_RESOURCE_MEDICATION_ADMINISTRATION = "MedicationAdministration";
  public static final String FHIR_RESOURCE_MEDICATION_STATEMENT = "MedicationStatement";
  public static final String FHIR_RESOURCE_MEDICATION = "Medication";
  public static final String FHIR_RESOURCE_DEPARTMENT_CASE = "DepartmentCase";
  public static final String FHIR_RESOURCE_ENCOUNTER = "Encounter";
  public static final String FHIR_RESOURCE_PROCEDURE = "Procedure";
  public static final String FHIR_RESOURCE_IMMUNIZATION = "Immunization";
  public static final String FHIR_RESOURCE_CONSENT = "Consent";
  public static final String FHIR_RESOURCE_DIAGNOSTIC_REPORT = "DiagnosticReport";
  public static final String FHIR_RESOURCE_BOTH_MEDICATION_RESOURCES = "Both";

  public static final String STEP_ENCOUNTER_INSTITUTION_KONTAKT =
      "stepProcessEncounterInstitutionContact";
  public static final String STEP_ENCOUNTER_DEPARTMENT_KONTAKT = "stepEncounterDepartmentCase";

  public static final String OMOP_DOMAIN_CONDITION = "Condition";
  public static final String OMOP_DOMAIN_OBSERVATION = "Observation";
  public static final String OMOP_DOMAIN_MEASUREMENT = "Measurement";
  public static final String OMOP_DOMAIN_PROCEDURE = "Procedure";
  public static final String OMOP_DOMAIN_DRUG = "Drug";
  public static final String OMOP_DOMAIN_GENDER = "Gender";

  public static final ImmutableList<String> FHIR_RESOURCE_ACCEPTABLE_EVENT_STATUS_LIST =
      ImmutableList.of("in-progress", "on-hold", "completed");
  public static final ImmutableList<String>
      FHIR_RESOURCE_MEDICATION_STATEMENT_ACCEPTABLE_STATUS_LIST =
          ImmutableList.of(
              "active",
              "completed",
              "on-hold",
              "entered-in-error",
              "intended",
              "stopped",
              "unknown",
              "not-taken");
  public static final ImmutableList<String> FHIR_RESOURCE_CONSENT_ACCEPTABLE_STATUS_LIST =
      ImmutableList.of("active");
  public static final ImmutableList<String> FHIR_RESOURCE_OBSERVATION_ACCEPTABLE_STATUS_LIST =
      ImmutableList.of("final");
  public static final ImmutableList<String> FHIR_RESOURCE_GECCO_OBSERVATION_ACCEPTABLE_VALUE_CODE =
      ImmutableList.of("373066001");
  public static final ImmutableList<String>
      FHIR_RESOURCE_GECCO_OBSERVATION_ECRF_PARAMETER_DOMAIN_OBSERVATION =
          ImmutableList.of("02", "03");
  public static final ImmutableList<String>
      FHIR_RESOURCE_GECCO_OBSERVATION_ECRF_PARAMETER_DOMAIN_MEASUREMENT = ImmutableList.of("06");

  public static final ImmutableList<String> FHIR_RESOURCE_DIAGNOSTIC_REPORT_ACCEPTABLE_STATUS_LIST =
      ImmutableList.of("final", "amended", "corrected", "appended");
  public static final ImmutableList<String>
      FHIR_RESOURCE_CONDITION_ACCEPTABLE_VERIFICATION_STATUS_LIST =
          ImmutableList.of("confirmed", "410605003");
  public static final ImmutableList<String> FHIR_RESOURCE_OBSERVATION_HISTORY_OF_TRAVEL_CODES =
      ImmutableList.of("8691-8", "443846001");
  public static final ImmutableList<String>
      FHIR_RESOURCE_GECCO_OBSERVATION_IN_MEASUREMENT_DOMAIN_CODES =
          ImmutableList.of("06", "85354-9", "75367002");
  public static final ImmutableList<String> FHIR_RESOURCE_GECCO_OBSERVATION_BLOOD_PRESSURE_CODES =
      ImmutableList.of("85354-9", "75367002");
  public static final ImmutableList<String> FHIR_RESOURCE_GECCO_OBSERVATION_SOFA_CODES =
      ImmutableList.of("06");

  private Constants() {}
}
