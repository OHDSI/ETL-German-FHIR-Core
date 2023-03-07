package org.miracum.etl.fhirtoomop.mapper.helpers;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import org.hl7.fhir.r4.model.CodeType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Consent.provisionComponent;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Enumeration;
import org.hl7.fhir.r4.model.Extension;
import org.hl7.fhir.r4.model.Period;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.Type;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.springframework.stereotype.Component;

/**
 * The ResourceCheckDataAbsentReason class is used to check whether the required element in FHIR
 * resource is filled using DataAbsentReason
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Component
public class ResourceCheckDataAbsentReason {

  private final FhirSystems fhirSystems;

  /**
   * Constructor for objects of the class ResourceCheckDataAbsentReason.
   *
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   */
  public ResourceCheckDataAbsentReason(FhirSystems fhirSystems) {

    this.fhirSystems = fhirSystems;
  }

  /**
   * Extract DataAbsentReason from stringElement
   *
   * @param stringElement stringElement from FHIR resource
   * @return DataAbsentReason from stringElement or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(StringType stringElement) {

    return stringElement.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from Coding
   *
   * @param coding coding from FHIR resource
   * @return DataAbsentReason from coding or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(Coding coding) {

    return coding.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from enumerationElement
   *
   * @param enumerationElement enumerationElement from FHIR resource
   * @return DataAbsentReason from enumerationElement or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(
      @SuppressWarnings("rawtypes") Enumeration enumerationElement) {

    return enumerationElement.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from codeElement
   *
   * @param codeElement codeElement from FHIR resource
   * @return DataAbsentReason from codeElement or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(CodeType codeElement) {

    return codeElement.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from dateTimeElement
   *
   * @param dateTimeElement dateTimeElement from FHIR resource
   * @return DataAbsentReason from dateTimeElement or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(DateTimeType dateTimeElement) {

    return dateTimeElement.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from typeElement
   *
   * @param typeElement typeElement from FHIR resource
   * @return DataAbsentReason from typeElement or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(Type typeElement) {

    return typeElement.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from periodElement
   *
   * @param periodElement periodElement from FHIR resource
   * @return DataAbsentReason from periodElement or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(Period periodElement) {

    return periodElement.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract DataAbsentReason from codeableConcept
   *
   * @param codeableConcept codeableConcept from FHIR resource
   * @return DataAbsentReason from codeableConcept or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(CodeableConcept codeableConcept) {

    return codeableConcept.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }
  /**
   * Extract DataAbsentReason from provisionComponent
   *
   * @param provisionComponent provisionComponent from FHIR resource
   * @return DataAbsentReason from provisionComponent or NULL if not exists
   */
  private Extension getDataAbsentReasonExtension(provisionComponent provisionComponent) {

    return provisionComponent.getExtensionByUrl(fhirSystems.getDataAbsentReason());
  }

  /**
   * Extract value of stringElement
   *
   * @param stringElement stringElement from FHIR resource
   * @return value of stringElement or NULL if not exists
   */
  public String getValue(StringType stringElement) {
    var dataAbsentReason = getDataAbsentReasonExtension(stringElement);
    if (dataAbsentReason == null) {
      return stringElement.getValue();
    }
    return null;
  }

  /**
   * Extract value of enumerationElement
   *
   * @param enumerationElement enumerationElement from FHIR resource
   * @return value of enumerationElement or NULL if not exists
   */
  public String getValue(@SuppressWarnings("rawtypes") Enumeration enumerationElement) {
    var dataAbsentReason = getDataAbsentReasonExtension(enumerationElement);
    if (dataAbsentReason == null) {
      return enumerationElement.getValueAsString();
    }
    return null;
  }

  /**
   * Extract value of codeElement
   *
   * @param codeElement codeElement from FHIR resource
   * @return value of codeElement or NULL if not exists
   */
  public String getValue(CodeType codeElement) {
    var dataAbsentReason = getDataAbsentReasonExtension(codeElement);
    if (dataAbsentReason == null) {
      return codeElement.getCode();
    }
    return null;
  }

  /**
   * Extract value of dateTimeElement
   *
   * @param dateTimeElement dateTimeElement from FHIR resource
   * @return value of dateTimeElement or NULL if not exists
   */
  public LocalDateTime getValue(DateTimeType dateTimeElement) {
    var dataAbsentReason = getDataAbsentReasonExtension(dateTimeElement);
    if (dataAbsentReason == null) {
      return new Timestamp(dateTimeElement.getValue().getTime()).toLocalDateTime();
    }
    return null;
  }

  /**
   * Extract value of typeElement
   *
   * @param typeElement typeElement from FHIR resource
   * @return value of typeElement or NULL if not exists
   */
  public Type getValue(Type typeElement) {
    var dataAbsentReason = getDataAbsentReasonExtension(typeElement);
    if (dataAbsentReason == null) {
      return typeElement;
    }
    return null;
  }

  /**
   * Extract value of periodElement
   *
   * @param periodElement periodElement from FHIR resource
   * @return value of periodElement or NULL if not exists
   */
  public Period getValue(Period periodElement) {
    var dataAbsentReason = getDataAbsentReasonExtension(periodElement);
    if (dataAbsentReason == null) {
      return periodElement;
    }
    return null;
  }

  /**
   * Extract value of codeableConcept
   *
   * @param codeableConcept codeableConcept from FHIR resource
   * @return value of codeableConcept or NULL if not exists
   */
  public CodeableConcept getValue(CodeableConcept codeableConcept) {
    var dataAbsentReason = getDataAbsentReasonExtension(codeableConcept);
    if (dataAbsentReason == null) {
      return codeableConcept;
    }
    return null;
  }
  /**
   * Extract value of provisionComponent
   *
   * @param provisionComponent provisionComponent from FHIR resource
   * @return value of provisionComponent or NULL if not exists
   */
  public provisionComponent getValue(provisionComponent provisionComponent) {
    var dataAbsentReason = getDataAbsentReasonExtension(provisionComponent);
    if (dataAbsentReason == null) {
      return provisionComponent;
    }
    return null;
  }

  /**
   * Extract value of coding
   *
   * @param coding coding from FHIR resource
   * @return value of coding or NULL if not exists
   */
  public Coding getValue(Coding coding) {
    var dataAbsentReason = getDataAbsentReasonExtension(coding);
    if (dataAbsentReason == null) {
      return coding;
    }
    return null;
  }
}
