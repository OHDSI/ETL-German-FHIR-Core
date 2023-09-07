package org.miracum.etl.fhirtoomop.mapper;

import org.hl7.fhir.instance.model.api.IBaseResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

/**
 * The interface FhirMapper defines a general map method for each item processor.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface FhirMapper<T extends IBaseResource> {
  OmopModelWrapper map(T resource, boolean isDeleted);
}
