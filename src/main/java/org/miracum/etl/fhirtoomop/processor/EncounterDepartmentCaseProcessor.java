package org.miracum.etl.fhirtoomop.processor;

import static org.miracum.etl.fhirtoomop.Constants.PROCESSING_RESOURCES_LOG;

import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Encounter;
import org.miracum.etl.fhirtoomop.mapper.EncounterDepartmentCaseMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

/**
 * The EncounterDepartmentCaseProcessor class represents the processing of FHIR Encounter
 * (department case) resources including the mapping between FHIR and OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class EncounterDepartmentCaseProcessor extends ResourceProcessor<Encounter> {

  /**
   * Constructor for objects of the class EncounterDepartmentCaseProcessor.
   *
   * @param mapper mapper which maps FHIR Encounter (department case) resources to OMOP CDM
   * @param fhirParser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   */
  public EncounterDepartmentCaseProcessor(
      EncounterDepartmentCaseMapper mapper, IParser fhirParser) {
    super(mapper, fhirParser);
  }

  /**
   * Processes FHIR Encounter (department case) resources and maps them to OMOP CDM.
   *
   * @param fhirPsqlResource FHIR resource and its metadata from FHIR Gateway
   * @return wrapper with objects to be written to OMOP CDM
   */
  @Override
  public OmopModelWrapper process(FhirPsqlResource fhirPsqlResource) {

    var r = fhirParser.parseResource(Encounter.class, fhirPsqlResource.getData());
    log.debug(PROCESSING_RESOURCES_LOG, r.getResourceType(), r.getId());
    return mapper.map(
        r,
        fhirPsqlResource.getIsDeleted() == null ? Boolean.FALSE : fhirPsqlResource.getIsDeleted());
  }
}
