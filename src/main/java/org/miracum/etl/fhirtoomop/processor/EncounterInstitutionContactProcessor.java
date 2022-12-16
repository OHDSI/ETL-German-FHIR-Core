package org.miracum.etl.fhirtoomop.processor;

import static org.miracum.etl.fhirtoomop.Constants.PROCESSING_RESOURCES_LOG;

import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Encounter;
import org.miracum.etl.fhirtoomop.mapper.EncounterInstitutionContactMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

/**
 * The EncounterInstitutionContactProcessor class represents the processing of FHIR Encounter
 * (administrative case/supply case) resources including the mapping between FHIR and OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class EncounterInstitutionContactProcessor extends ResourceProcessor<Encounter> {

  /**
   * Constructor for objects of the class EncounterInstitutionContactProcessor.
   *
   * @param mapper mapper which maps FHIR Encounter (administrative case/supply case) resources to
   *     OMOP CDM
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   */
  public EncounterInstitutionContactProcessor(
      EncounterInstitutionContactMapper mapper, IParser parser) {
    super(mapper, parser);
  }

  /**
   * Processes FHIR Encounter (administrative case/supply case) resources and maps them to OMOP CDM.
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
