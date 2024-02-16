package org.miracum.etl.fhirtoomop.mapper.helpers;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;

/**
 * Initializing a global metrics register for different counters.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public class MapperMetrics {

  public static Counter setStatusErrorCounter(String stepName){
    return Counter.builder("resource.status.error")
            .description("Status is not acceptable")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  /**
   * Initialize a Counter for 'no start data found'
   *
   * @param stepName the name of a Step in this Job
   * @return a new Counter
   */
  public static Counter setNoStartDateCounter(String stepName) {
    return Counter.builder("no.start.date")
        .description("No startdate found")
        .tag("type", stepName)
        .register(Metrics.globalRegistry);
  }

  /**
   * Initialize a Counter for 'no referenced person found'
   *
   * @param stepName the name of a Step in this Job
   * @return a new Counter
   */
  public static Counter setNoPersonIdCounter(String stepName) {
    return Counter.builder("no.person.id")
        .description("No referenced person found")
        .tag("type", stepName)
        .register(Metrics.globalRegistry);
  }

  /**
   * Initialize a Counter for 'invalid source code'
   *
   * @param stepName the name of a Step in this Job
   * @return a new Counter
   */
  public static Counter setInvalidCodeCounter(String stepName) {
    return Counter.builder("invalid.code")
        .description("The source code is invalid in OMOP")
        .tag("type", stepName)
        .register(Metrics.globalRegistry);
  }

  /**
   * Initialize a Counter for 'no source code found'
   *
   * @param stepName the name of a Step in this Job
   * @return a new Counter
   */
  public static Counter setNoCodeCounter(String stepName) {
    return Counter.builder("no.source.code")
        .description("No source code found")
        .tag("type", stepName)
        .register(Metrics.globalRegistry);
  }

  /**
   * Initialize a Counter for 'no FHIR reference found'
   *
   * @param stepName the name of a Step in this Job
   * @return a new Counter
   */
  public static Counter setNoFhirReferenceCounter(String stepName) {
    return Counter.builder("no.fhir.reference")
        .description("No FHIR reference found")
        .tag("type", stepName)
        .register(Metrics.globalRegistry);
  }

  /**
   * Initialize a Counter for 'Number of processed FHIR resources'
   *
   * @return a new Counter
   */
  public static Counter setProcessedFhirRessourceCounter() {
    return Counter.builder("batch.fhir.resources.processed.total")
        .description("Number of processed FHIR resources")
        .tag("fhir-resource", "processed")
        .register(Metrics.globalRegistry);
  }

  public static Counter setDeletedFhirRessourceCounter(String stepName) {
    return Counter.builder("batch.fhir.resources.deleted")
        .description("Number of deleted FHIR resources")
        .tag("deleted", stepName)
        .register(Metrics.globalRegistry);
  }
}
