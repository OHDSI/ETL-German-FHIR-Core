package org.miracum.etl.fhirtoomop.listeners;

import io.micrometer.core.instrument.Counter;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.springframework.batch.core.ItemProcessListener;

public class FhirResourceProcessListener
    implements ItemProcessListener<FhirPsqlResource, OmopModelWrapper> {
  private static final Counter totalProcessedFhirResources =
      MapperMetrics.setProcessedFhirRessourceCounter();

  @Override
  public void beforeProcess(FhirPsqlResource item) {
    // TODO Auto-generated method stub

  }

  @Override
  public void afterProcess(FhirPsqlResource item, OmopModelWrapper result) {
    totalProcessedFhirResources.increment();
  }

  @Override
  public void onProcessError(FhirPsqlResource item, Exception e) {
    // TODO Auto-generated method stub

  }
}
