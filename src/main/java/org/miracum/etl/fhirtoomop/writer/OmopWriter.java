package org.miracum.etl.fhirtoomop.writer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.batch.item.ItemWriter;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.listener.RetryListenerSupport;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;

/**
 * The OmopWriter class is used to write the data from FHIR resources to OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
@Component
public class OmopWriter implements ItemWriter<OmopModelWrapper> {

  private static final Counter resourcesProcessedTotal =
      Metrics.counter("batch.fhir.resources.processed.total", "fhir-resource", "processed");

  private final RetryTemplate retryTemplate = new RetryTemplate();
  private final OmopRepository repository;

  /**
   * Constructor for objects of the class OmopWriter.
   *
   * @param repository for OMOP CDM tables
   */
  public OmopWriter(OmopRepository repository) {
    this.repository = repository;

    var backOffPolicy = new FixedBackOffPolicy();
    backOffPolicy.setBackOffPeriod(10000);
    var retryPolicy = new SimpleRetryPolicy();
    retryPolicy.setMaxAttempts(10);

    retryTemplate.setBackOffPolicy(backOffPolicy);
    retryTemplate.setRetryPolicy(retryPolicy);

    retryTemplate.registerListener(
        new RetryListenerSupport() {
          @Override
          public <T, E extends Throwable> void onError(
              RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            log.error(
                "Trying to write data caused error. {} attempt.",
                context.getRetryCount(),
                throwable);
          }
        });
  }

  /**
   * Writes the data from the FHIR resources to OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private Optional<Void> writeOmopChunk(List<? extends OmopModelWrapper> entries) {

    writeMedicationIdMap(entries);
    writePostProcessMap(entries);
    writePerson(entries);
    writeOrganization(entries);
    writeVisitDetail(entries);
    writeVisitOcc(entries);
    writeObservation(entries);
    writeConditionOcc(entries);
    writeProcedureOcc(entries);
    writeDrugExposure(entries);
    writeMeasurement(entries);
    writeDeviceExposure(entries);

    return Optional.empty();
  }

  /**
   * Writes the data from FHIR resources to the medication_id_map table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeMedicationIdMap(List<? extends OmopModelWrapper> entries) {
    var medicationIdMap =
        entries.stream()
            .filter(entry -> entry.getMedicationIdMap() != null)
            .map(OmopModelWrapper::getMedicationIdMap)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!medicationIdMap.isEmpty()) {
      log.info("Inserting {} rows into medication_id_map table", medicationIdMap.size());

      repository.getMedicationIdRepository().saveAll(medicationIdMap);
    }
  }

  /**
   * Writes the data from FHIR resources to the post_process_map table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writePostProcessMap(List<? extends OmopModelWrapper> entries) {
    var postProcessMap =
        entries.stream()
            .filter(entry -> entry.getPostProcessMap() != null)
            .map(OmopModelWrapper::getPostProcessMap)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!postProcessMap.isEmpty()) {
      log.info("Inserting {} rows into post_process_map table", postProcessMap.size());

      repository.getPostProcessMapRepository().saveAll(postProcessMap);
    }
  }

  /**
   * Writes the data from FHIR resources to the person table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writePerson(List<? extends OmopModelWrapper> entries) {
    var persons =
        entries.stream()
            .filter(entry -> entry.getPerson() != null)
            .map(OmopModelWrapper::getPerson)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!persons.isEmpty()) {
      log.info("Inserting {} rows into person table", persons.size());

      repository.getPersonRepository().saveAll(persons);
    }
  }

  private void writeOrganization(List<? extends OmopModelWrapper> entries) {
    var organization =
            entries.stream()
                    .filter(entry -> entry.getCareSite() != null)
                    .map(OmopModelWrapper::getCareSite)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
    if(!organization.isEmpty()){
      log.info("Inserting {} rows into care_site table",organization.size());
      repository.getCareSiteRepository().saveAll(organization);
    }
  }

  /**
   * Writes the data from FHIR resources to the visit_detail table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeVisitDetail(List<? extends OmopModelWrapper> entries) {
    var visitDetails =
        entries.stream()
            .filter(entry -> entry.getVisitDetail() != null)
            .map(OmopModelWrapper::getVisitDetail)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!visitDetails.isEmpty()) {
      log.info("Inserting {} rows into visit_detail table", visitDetails.size());

      repository.getVisitDetailRepository().saveAll(visitDetails);
    }
  }

  /**
   * Writes the data from FHIR resources to the visit_occurrence table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeVisitOcc(List<? extends OmopModelWrapper> entries) {
    var visits =
        entries.stream()
            .filter(entry -> entry.getVisitOccurrence() != null)
            .map(OmopModelWrapper::getVisitOccurrence)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!visits.isEmpty()) {
      log.info("Inserting {} rows into visit_occurrence table", visits.size());

      repository.getVisitOccRepository().saveAll(visits);
    }
  }

  /**
   * Writes the data from FHIR resources to the observation table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeObservation(List<? extends OmopModelWrapper> entries) {
    log.info("write------------{}",entries.size());
    var observations =
        entries.stream()
            .filter(entry -> entry.getObservation() != null)
            .map(OmopModelWrapper::getObservation)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!observations.isEmpty()) {
      log.info("Inserting {} rows into observation table", observations.size());

      repository.getObservationRepository().saveAll(observations);
    }
  }

  /**
   * Writes the data from FHIR resources to the condition_occurrence table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeConditionOcc(List<? extends OmopModelWrapper> entries) {
    var conditionOccurrence =
        entries.stream()
            .filter(entry -> entry.getConditionOccurrence() != null)
            .map(OmopModelWrapper::getConditionOccurrence)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!conditionOccurrence.isEmpty()) {
      log.info("Inserting {} rows into condition_occurrence table", conditionOccurrence.size());

      repository.getConditionOccRepository().saveAll(conditionOccurrence);
    }
  }

  /**
   * Writes the data from FHIR resources to the procedure_occurrence table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeProcedureOcc(List<? extends OmopModelWrapper> entries) {
    var procedures =
        entries.stream()
            .filter(entry -> entry.getProcedureOccurrence() != null)
            .map(OmopModelWrapper::getProcedureOccurrence)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!procedures.isEmpty()) {
      log.info("Inserting {} rows into procedure_occurrence table", procedures.size());

      repository.getProcedureOccRepository().saveAll(procedures);
    }
  }

  /**
   * Writes the data from FHIR resources to the drug_exposure table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeDrugExposure(List<? extends OmopModelWrapper> entries) {
    var drugExposures =
        entries.stream()
            .filter(entry -> entry.getDrugExposure() != null)
            .map(OmopModelWrapper::getDrugExposure)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!drugExposures.isEmpty()) {
      log.info("Inserting {} rows into drug_exposure table", drugExposures.size());

      repository.getDrugExposureRepository().saveAll(drugExposures);
    }
  }

  /**
   * Writes the data from FHIR resources to the measurement table in OMOP CDM.
   *
   * @param entries list of elements to be written to OMOP CDM
   */
  private void writeMeasurement(List<? extends OmopModelWrapper> entries) {
    var measurements =
        entries.stream()
            .filter(entry -> entry.getMeasurement() != null)
            .map(OmopModelWrapper::getMeasurement)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());

    if (!measurements.isEmpty()) {
      log.info("Inserting {} rows into measurement table", measurements.size());

      repository.getMeasurementRepository().saveAll(measurements);
    }
  }

  private void writeDeviceExposure(List<? extends OmopModelWrapper> entries) {
    var deviceExposure =
        entries.stream()
            .filter(entry -> entry.getDeviceExposure() != null)
            .map(OmopModelWrapper::getDeviceExposure)
            .flatMap(List::stream)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    if (!deviceExposure.isEmpty()) {
      log.info("Inserting {} rows into device_exposure table", deviceExposure.size());

      repository.getDeviceExposureRepository().saveAll(deviceExposure);
    }
  }

  /**
   * Executes the writing of the data from FHIR resources to OMOP CDM.
   *
   * @param items list of elements to be written to OMOP CDM
   */
  @Override
  public void write(List<? extends OmopModelWrapper> items) {
    retryTemplate.execute(context -> writeOmopChunk(items));
    log.info("Total: {} FHIR resources processed", (int) resourcesProcessedTotal.count());
  }
}
