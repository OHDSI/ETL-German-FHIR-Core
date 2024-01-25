package org.miracum.etl.fhirtoomop;

import static org.miracum.etl.fhirtoomop.Constants.STEP_ENCOUNTER_DEPARTMENT_KONTAKT;
import static org.miracum.etl.fhirtoomop.Constants.STEP_ENCOUNTER_INSTITUTION_KONTAKT;

import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.gclient.IQuery;
import ca.uhn.fhir.rest.param.DateRangeParam;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.NoArgsConstructor;
import org.hl7.fhir.instance.model.api.IBaseBundle;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Encounter;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

@NoArgsConstructor
public class FhirServerItemReader extends AbstractPagingItemReader<FhirPsqlResource>
    implements InitializingBean {
  private IGenericClient client;
  private String resourceTypeName;
  private String beginDate;
  private String endDate;
  private String stepName;
  private IParser fhirParser;
  private Map<String, Object> startAfterValues;
  private static final String START_AFTER_VALUE = "start.after";
  private Map<String, Object> previousStartAfterValues;

  private Bundle firstBundle;
  private Bundle nextBundle;

  public void setFhirClient(IGenericClient client) {
    this.client = client;
  }

  public void setFhirParser(IParser fhirParser) {
    this.fhirParser = fhirParser;
  }

  public void setBeginDate(String beginDate) {
    this.beginDate = beginDate;
  }

  public void setEndDate(String endDate) {
    this.endDate = endDate;
  }

  public void setStepName(String stepName) {
    this.stepName = stepName;
  }

  public void setResourceTypeClass(String resourceTypeName) {
    this.resourceTypeName = resourceTypeName;
  }

  private DateRangeParam generateDateRange() {
    if (LocalDate.parse("1800-01-01").equals(LocalDate.parse(beginDate))
        && LocalDate.parse("2099-12-31").equals(LocalDate.parse(endDate))) {
      return null;
    }
    return new DateRangeParam(beginDate, endDate);
  }

  private Bundle readPageOfBundle() {
    var dateRange = generateDateRange();
    var searchQuery = searchQuery();
    if (dateRange == null) {
      return searchQuery.execute();
    }
    return searchQuery.lastUpdated(dateRange).execute();
  }

  private IQuery<Bundle> searchQuery() {
    var query =
        client
            .search()
            .forResource(resourceTypeName)
            .returnBundle(Bundle.class)
            .count(getPageSize());
//    if (stepName.equals(STEP_ENCOUNTER_DEPARTMENT_KONTAKT)) {
//      return query.and(Encounter.TYPE.exactly().code("abteilungskontakt"));
//    } else if (stepName.equals(STEP_ENCOUNTER_INSTITUTION_KONTAKT)) {
//      return query.and(Encounter.TYPE.exactly().code("einrichtungskontakt"));
//    }
    return query;
  }

  @Override
  public void afterPropertiesSet() throws Exception {
    super.afterPropertiesSet();
    Assert.notNull(resourceTypeName, "Name of resource may not be null");
    this.firstBundle = readPageOfBundle();
    this.nextBundle = firstBundle;
  }

  @Override
  protected void doReadPage() {
    if (results == null) {
      results = new CopyOnWriteArrayList<>();
    } else {
      results.clear();
    }
    List<FhirPsqlResource> newResources;

    if (getPage() == 0) {
      newResources = resourceTransform(firstBundle);
    } else if (startAfterValues != null) {
      previousStartAfterValues = startAfterValues;
      if (nextBundle.getLink(IBaseBundle.LINK_NEXT) != null) {
        var partialBundle = client.loadPage().next(nextBundle).execute();
        newResources = resourceTransform(partialBundle);
        this.nextBundle = partialBundle;

      } else {
        newResources = Collections.emptyList();
      }
    } else {
      newResources = Collections.emptyList();
    }
    results.addAll(newResources);
  }

  private List<FhirPsqlResource> resourceTransform(Bundle bundle) {

    if (bundle == null || bundle.isEmpty()) {
      return Collections.emptyList();
    }
    if (!bundle.hasEntry()) {
      return Collections.emptyList();
    }
    var entries = bundle.getEntry();
    if (entries.isEmpty()) {
      return Collections.emptyList();
    }
    List<FhirPsqlResource> resources = new ArrayList<>();
    for (var entry : entries) {
      FhirPsqlResource newResource = new FhirPsqlResource();
      var resource = entry.getResource();
      var resourceData = fhirParser.encodeResourceToString(resource);
      newResource.setData(resourceData);
      newResource.setFhirId(resource.getIdElement().getId());
      newResource.setType(resource.getResourceType().name());
      resources.add(newResource);
    }
    return resources;
  }

  @Override
  public void update(ExecutionContext executionContext) throws ItemStreamException {
    super.update(executionContext);
    if (isSaveState()) {
      if (isAtEndOfPage() && startAfterValues != null) {
        // restart on next page
        executionContext.put(getExecutionContextKey(START_AFTER_VALUE), startAfterValues);
      } else if (previousStartAfterValues != null) {
        // restart on current page
        executionContext.put(getExecutionContextKey(START_AFTER_VALUE), previousStartAfterValues);
      }
    }
  }

  private boolean isAtEndOfPage() {
    return getCurrentItemCount() % getPageSize() == 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public void open(ExecutionContext executionContext) {
    if (isSaveState()) {
      startAfterValues =
          (Map<String, Object>) executionContext.get(getExecutionContextKey(START_AFTER_VALUE));

      if (startAfterValues == null) {
        startAfterValues = new LinkedHashMap<>();
      }
    }

    super.open(executionContext);
  }

  @Override
  protected void doJumpToPage(int itemIndex) {
    // TODO Auto-generated method stub

  }
}
