package org.miracum.etl.fhirtoomop.mapper;

import java.util.List;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;

/**
 * The interface FhirMapper defines a general map method for each item processor.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface FhirMapper<T extends IBaseResource> {
  OmopModelWrapper map(T resource, boolean isDeleted);

  public default void addToObservationList(
      List<OmopObservation> observationList, OmopObservation observationEntity) {
    if (observationList.contains(observationEntity)) {
      return;
    }
    observationList.add(observationEntity);
  }

  public default void addToPostProcessMap(
      List<PostProcessMap> postProcessMapList, PostProcessMap postProcessMapEntity) {
    if (postProcessMapList.contains(postProcessMapEntity)) {
      return;
    }
    postProcessMapList.add(postProcessMapEntity);
  }
}
