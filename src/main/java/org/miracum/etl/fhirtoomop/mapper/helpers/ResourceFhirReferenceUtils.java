package org.miracum.etl.fhirtoomop.mapper.helpers;

import static org.miracum.etl.fhirtoomop.Constants.FHIR_RESOURCE_CONSENT;

import ca.uhn.fhir.fhirpath.IFhirPath;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.Reference;
import org.hl7.fhir.r4.model.Resource;
import org.hl7.fhir.r4.model.StringType;
import org.miracum.etl.fhirtoomop.config.FhirSystems;

/**
 * The ResourceFhirReferenceUtils class is used to extract references to other FHIR resources from
 * the processing FHIR resource.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class ResourceFhirReferenceUtils {

  private final IFhirPath fhirPath;
  private final FhirSystems fhirSystems;

  /**
   * Constructor for objects of the class ResourceFhirReferenceUtils.
   *
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @param fhirSystems reference to naming and coding systems used in FHIR resources
   */
  public ResourceFhirReferenceUtils(IFhirPath fhirPath, FhirSystems fhirSystems) {
    this.fhirPath = fhirPath;
    this.fhirSystems = fhirSystems;
  }

  /**
   * Extracts the identifier of a referenced FHIR Patient resource from a FHIR resource.
   *
   * @param resource FHIR resource
   * @return identifier of a referenced FHIR Patient resource
   */
  public String getSubjectReferenceIdentifier(IBaseResource resource) {
    var subjectIdentifierByTypePath = "subject.identifier.value";
    var subjectIdentifier =
        fhirPath.evaluateFirst(resource, subjectIdentifierByTypePath, StringType.class);

    var patientIdentifierByTypePath = "patient.identifier.value";
    var patientIdentifier =
        fhirPath.evaluateFirst(resource, patientIdentifierByTypePath, StringType.class);

    if (!subjectIdentifier.isPresent() && !patientIdentifier.isPresent()) {
      return null;
    } else {
      return "pat-"
          + (subjectIdentifier.isPresent()
              ? subjectIdentifier.get().getValue()
              : patientIdentifier.get().getValue());
    }
  }

  /**
   * Extracts the logical id of a referenced FHIR Patient resource from a FHIR resource.
   *
   * @param resource FHIR resource
   * @return logical id of a referenced FHIR Patient resource
   */
  public String getSubjectReferenceLogicalId(IBaseResource resource) {
    var subjectReferencePath = "subject.reference";
    var subjectLogicalId = fhirPath.evaluateFirst(resource, subjectReferencePath, StringType.class);

    var patientReferencePath = "patient.reference";
    var patientLogicalId = fhirPath.evaluateFirst(resource, patientReferencePath, StringType.class);

    if (!subjectLogicalId.isPresent() && !patientLogicalId.isPresent()) {
      return null;
    } else {
      var reference =
          new Reference(
              subjectLogicalId.isPresent()
                  ? subjectLogicalId.get().getValue()
                  : patientLogicalId.get().getValue());
      return "pat-" + reference.getReferenceElement().getIdPart();
    }
  }

  /**
   * Extracts the identifier of a referenced FHIR Encounter resource from a FHIR resource.
   *
   * @param resource FHIR resource
   * @return identifier of a referenced FHIR Encounter resource
   */
  public String getEncounterReferenceIdentifier(IBaseResource resource) {
    var identifierByTypePath = "encounter.identifier.value";
    var identifier = fhirPath.evaluateFirst(resource, identifierByTypePath, StringType.class);

    if (identifier.isPresent()) {

      return "enc-" + identifier.get().getValue();
    }

    return null;
  }

  /**
   * Extracts the logical id of a referenced FHIR Encounter resource from a FHIR resource.
   *
   * @param resource FHIR resource
   * @return logical id of a referenced FHIR Encounter resource
   */
  public String getEncounterReferenceLogicalId(IBaseResource resource) {

    var referencePath = "encounter.reference";
    var logicalId = fhirPath.evaluateFirst(resource, referencePath, StringType.class);

    if (logicalId.isPresent()) {
      var reference = new Reference(logicalId.get().getValue());

      return "enc-" + reference.getReferenceElement().getIdPart();
    }

    return null;
  }
  /**
   * Extracts the identifier from a FHIR resource.
   *
   * @param resource processing FHIR resource
   * @param typeCode code of the identifier type of the FHIR resource
   * @return identifier from the processing FHIR resource
   */
  public String extractIdentifier(Resource resource, String typeCode) {
    var identifierByTypePath =
        String.format(
            "identifier.where(type.coding.where(system='%s' and code='%s').exists()).value",
            fhirSystems.getIdentifierType(), typeCode);
    var identifierList = fhirPath.evaluate(resource, identifierByTypePath, StringType.class);

    if (identifierList.isEmpty()) {
      for (String identifierSystem : fhirSystems.getIdentifierSystem()) {
        identifierByTypePath =
            String.format("identifier.where(system='%s').value", identifierSystem);
        identifierList = fhirPath.evaluate(resource, identifierByTypePath, StringType.class);
        if (!identifierSystem.isEmpty()) {
          break;
        }
      }
    }
    if (identifierList.isEmpty()) {
      return null;
    }

    var identifer =
        identifierList.stream().map(Objects::toString).filter(StringUtils::isNotBlank).findFirst();
    if (identifer.isPresent()) {
      var prefix = getResourceTypePrefix(resource);
      if (prefix != null) {
        return prefix + identifer.get();
      }
    }

    return null;
  }

  /**
   * Extracts the logical id from a FHIR resource.
   *
   * @param resource processing FHIR resource
   * @return logical id from the processing FHIR resource
   */
  public String extractId(Resource resource) {
    if (!resource.hasId()) {
      log.debug("Given [{}] resource has no identifying source value", resource.getResourceType());
      return null;
    }
    var prefix = getResourceTypePrefix(resource);
    if (prefix != null) {
      return prefix + resource.getIdElement().getIdPart();
    }
    return null;
  }

  private String getResourceTypePrefix(IBaseResource resource) {
    var resourceTypeName = resource.fhirType().split("(?=\\p{Upper})");
    switch (resourceTypeName.length) {
      case 1:
        if (resourceTypeName[0].equals(FHIR_RESOURCE_CONSENT)) {
          return resourceTypeName[0].substring(0, 4).toLowerCase() + "-";
        }
        return resourceTypeName[0].substring(0, 3).toLowerCase() + "-";
      case 2:
        return (resourceTypeName[0].substring(0, 2) + resourceTypeName[1].substring(0, 1))
                .toLowerCase()
            + "-";
      default:
        log.error("No [Resource Type] found, invalid resource. Please check!");
        return null;
    }
  }
  /**
   * Extracts the first found identifier from the FHIR resource.
   *
   * @param resource FHIR resource
   * @return first found identifier from FHIR resource
   */
  public String extractResourceFirstIdentifier(Resource resource) {
    var identifierPath = "identifier.value";
    var identifierList = fhirPath.evaluate(resource, identifierPath, StringType.class);
    if (identifierList.isEmpty()) {
      return null;
    }

    var identifer =
        identifierList.stream().map(Objects::toString).filter(StringUtils::isNotBlank).findFirst();
    if (identifer.isPresent()) {
      var prefix = getResourceTypePrefix(resource);
      if (prefix != null) {
        return prefix + identifer.get();
      }
    }

    return null;
  }
}
