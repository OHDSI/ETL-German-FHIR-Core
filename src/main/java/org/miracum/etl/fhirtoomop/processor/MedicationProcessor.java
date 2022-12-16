package org.miracum.etl.fhirtoomop.processor;

import static org.miracum.etl.fhirtoomop.Constants.PROCESSING_RESOURCES_LOG;

import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Medication;
import org.miracum.etl.fhirtoomop.mapper.MedicationMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

/**
 * The MedicationProcessor class represents the processing of FHIR Medication resources including
 * the mapping between FHIR and OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class MedicationProcessor extends ResourceProcessor<Medication> {

  /**
   * Constructor for objects of the class MedicationProcessor.
   *
   * @param mapper mapper which maps FHIR Medication resources to OMOP CDM
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   */
  public MedicationProcessor(MedicationMapper mapper, IParser parser) {
    super(mapper, parser);
  }

  /**
   * Processes FHIR Medication resources and maps them to OMOP CDM.
   *
   * @param fhirPsqlResource FHIR resource and its metadata from FHIR Gateway
   * @return wrapper with objects to be written to OMOP CDM
   */
  @Override
  public OmopModelWrapper process(FhirPsqlResource fhirPsqlResource) {

    var r = fhirParser.parseResource(Medication.class, fhirPsqlResource.getData());
    log.debug(PROCESSING_RESOURCES_LOG, r.getResourceType(), r.getId());
    return mapper.map(
        r,
        fhirPsqlResource.getIsDeleted() == null ? Boolean.FALSE : fhirPsqlResource.getIsDeleted());
  }
}
