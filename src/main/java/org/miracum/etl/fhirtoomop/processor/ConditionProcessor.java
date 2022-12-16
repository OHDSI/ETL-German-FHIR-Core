package org.miracum.etl.fhirtoomop.processor;

import static org.miracum.etl.fhirtoomop.Constants.PROCESSING_RESOURCES_LOG;

import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Condition;
import org.miracum.etl.fhirtoomop.mapper.ConditionMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

/**
 * The ConditionProcessor class represents the processing of FHIR Condition resources including the
 * mapping between FHIR and OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class ConditionProcessor extends ResourceProcessor<Condition> {

  /**
   * Constructor for objects of the class ConditionProcessor.
   *
   * @param mapper mapper which maps FHIR Conditions resources to OMOP CDM
   * @param parser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   */
  public ConditionProcessor(ConditionMapper mapper, IParser parser) {
    super(mapper, parser);
  }

  /**
   * Processes FHIR Condition resources and maps them to OMOP CDM.
   *
   * @param fhirPsqlResource FHIR resource and its metadata from FHIR Gateway
   * @return wrapper with objects to be written to OMOP CDM
   */
  @Override
  public OmopModelWrapper process(FhirPsqlResource fhirPsqlResource) {

    var r = fhirParser.parseResource(Condition.class, fhirPsqlResource.getData());
    log.debug(PROCESSING_RESOURCES_LOG, r.getResourceType(), r.getId());
    return mapper.map(
        r,
        fhirPsqlResource.getIsDeleted() == null ? Boolean.FALSE : fhirPsqlResource.getIsDeleted());
  }
}
