package org.miracum.etl.fhirtoomop.processor;

import ca.uhn.fhir.parser.IParser;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.miracum.etl.fhirtoomop.mapper.FhirMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.springframework.batch.item.ItemProcessor;

/**
 * The ResourceProcessor class represents the processing of FHIR resources including the mapping
 * between FHIR and OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public abstract class ResourceProcessor<E extends IBaseResource>
    implements ItemProcessor<FhirPsqlResource, OmopModelWrapper> {
  protected final FhirMapper<E> mapper;
  protected final IParser fhirParser;

  /**
   * Constructor for objects of the class ResourceProcessor.
   *
   * @param mapper mapper which maps FHIR resources to OMOP CDM
   * @param fhirParser parser which converts between the HAPI FHIR model/structure objects and their
   *     respective String wire format (JSON)
   */
  public ResourceProcessor(FhirMapper<E> mapper, IParser fhirParser) {
    this.mapper = mapper;
    this.fhirParser = fhirParser;
  }
}
