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

  public static Counter setNoMatchingEncounterCount(String stepName){
    return Counter.builder("no.matching.encounter")
            .description("No Matching Encounter found for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoResultHistoryTravelFoundCount(String stepName){
    return Counter.builder("history.of.travel.result.not.found")
            .description("Result for History of travel not found for the source")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoAcceptableResultHistoryOfTravelFoundCount(String stepName){
    return Counter.builder("no.acceptable.history.of.travel.found")
            .description("History of travel result not acceptable for the source")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setAvailableInfoHistoryOfTravelNotFoundCount(String stepName){
    return Counter.builder("no.available.info.history.of.travel.found")
            .description("No available information found for history of travel for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setValueNotFoundCount(String stepName){
    return Counter.builder("no.value.found")
            .description("No valueQuantity or valueCodeableConcept found for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoInterpretationFoundCount(String stepName){
    return Counter.builder("no.interpretation.found")
            .description("No Interpretation found in the resource.")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoReferenceRangeFoundCount(String stepName){
    return Counter.builder("no.reference.range.found")
            .description("No Reference Range found in the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setMissingHighRangeCount(String stepName){
    return Counter.builder("missing.high.range")
            .description("Missing high range for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setMissingLowRangeCount(String stepName){
    return Counter.builder("missing.low.range")
            .description("Missing low range for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setCategoryNotFoundCount(String stepName){
    return Counter.builder("category.not.found")
            .description("No category found for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setStatusErrorCounter(String stepName){
    return Counter.builder("resource.status.error")
            .description("Status is not acceptable")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setVerificationStatusNotAcceptableCounter(String stepName){
    return Counter.builder("verification.status.not.acceptable")
            .description("Verification Status is not acceptable")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setICDCodeInvalidCounter(String stepName){
    return Counter.builder("icd.code.invalid")
            .description("ICD Code in resource is not valid")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setDiagnosticConfidenceNotFoundCounter(String stepName){
    return Counter.builder("diagnostic.confidence.not.found")
            .description("No Diagnostic Confidence found for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setStatusNotAcceptableCounter(String stepName){
    return Counter.builder("status.not.acceptable")
            .description("The status is not acceptable")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setUnableToExtractResourceCounter(String stepName){
    return Counter.builder("unable.extract.resource")
            .description("Unable to extract the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoMatchingVisitOccurenceCounter(String stepName){
    return Counter.builder("no.matching.visitOccurence")
            .description("No matching VisitOccurence for the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoStartDateFoundInLocationCounter(String stepName){
    return Counter.builder("no.start.date.found.in.location")
            .description("No start date found in the location")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoLocationReferenceFoundCounter(String stepName){
    return Counter.builder("no.location.reference.found")
            .description("No Location Reference found in the resource")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoDepartmentCodeFoundCounter(String stepName){
    return Counter.builder("no.department.code.found")
            .description("No Department Code Found")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setInvalidDoseCounter(String stepName){
    return Counter.builder("invalid.dose.found")
            .description("Unable to determine the dose")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setInvalidDosageCounter(String stepName){
    return Counter.builder("invalid.dosage.found")
            .description("Unable to determine the dosage")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setInvalidRouteValueCounter(String stepName){
    return Counter.builder("invalid.route.counter")
            .description("Unable to determine the route value")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setNoBirthDateFoundCounter(String stepName){
    return Counter.builder("no.birth.date.found")
            .description("No Birth Date Found")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setInvalidStringLengthCounter(String stepName){
    return Counter.builder("invalid.string.length")
            .description("String is longer than allowed")
            .tag("type", stepName)
            .register(Metrics.globalRegistry);
  }

  public static Counter setInvalidBirthDateCounter(String stepName){
    return Counter.builder("invalid.birth.date")
            .description("Unable to calculate birth date")
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
