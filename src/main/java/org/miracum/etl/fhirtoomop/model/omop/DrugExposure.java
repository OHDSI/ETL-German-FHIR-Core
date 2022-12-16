package org.miracum.etl.fhirtoomop.model.omop;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The DrugExposure class describes the structure of the drug_exposure table in OMOP CDM. The drug
 * exposure domain captures records about the utilization of a Drug when ingested or otherwise
 * introduced into the body.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "drug_exposure")
public class DrugExposure {

  /** A system-generated unique identifier for each Drug utilization event. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "drug_exposure_id")
  private Long drugExposureId;

  /** A foreign key identifier to the person who is subjected to the Drug. */
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key that refers to a Standard Concept identifier in the Standardized Vocabularies for
   * the Drug concept.
   */
  @Column(name = "drug_concept_id")
  private int drugConceptId;

  /** The start date for the current instance of Drug utilization. */
  @Column(name = "drug_exposure_start_date")
  private LocalDate drugExposureStartDate;

  /** The start date and time for the current instance of Drug utilization. */
  @Column(name = "drug_exposure_start_datetime")
  private LocalDateTime drugExposureStartDatetime;

  /** The end date for the current instance of Drug utilization. */
  @Column(name = "drug_exposure_end_date")
  private LocalDate drugExposureEndDate;

  /** The end date and time for the current instance of Drug utilization. */
  @Column(name = "drug_exposure_end_datetime")
  private LocalDateTime drugExposureEndDatetime;

  /** The end date of the drug exposure as it appears in the source data. */
  @Column(name = "verbatim_end_date")
  private LocalDate verbatimEndDate;

  /**
   * A foreign key to the predefined Concept identifier in the Standardized Vocabularies reflecting
   * the type of Drug Exposure recorded.
   */
  @Column(name = "drug_type_concept_id")
  private int drugTypeConceptId;

  /** The reason the Drug was stopped. */
  @Column(name = "stop_reason")
  private String stopReason;

  /** The number of refills after the initial prescription. */
  private Integer refills;

  /** The quantity of drug as recorded in the original prescription or dispensing record. */
  private BigDecimal quantity;

  /**
   * The number of days of supply of the medication as recorded in the original prescription or
   * dispensing record.
   */
  @Column(name = "days_supply")
  private Integer daysSupply;

  /**
   * The directions ("signetur") on the Drug prescription as recorded in the original prescription
   * (and printed on the container) or dispensing record.
   */
  private String sig;

  /**
   * A foreign key to a predefined concept in the Standardized Vocabularies reflecting the route of
   * administration.
   */
  @Column(name = "route_concept_id")
  private Integer routeConceptId;

  /**
   * An identifier assigned to a particular quantity or lot of Drug product from the manufacturer.
   */
  @Column(name = "lot_number")
  private String lotNumber;

  /**
   * A foreign key to the provider in the provider table who initiated (prescribed or administered)
   * the Drug Exposure.
   */
  @Column(name = "provider_id")
  private Integer providerId;

  /** A foreign key to the visit in the visit table during which the Drug Exposure was initiated. */
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** The visit_detail record during which the drug exposure occurred. */
  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  /** The source code for the Drug as it appears in the source data. */
  @Column(name = "drug_source_value")
  private String drugSourceValue;

  /** A foreign key to a Drug Concept that refers to the code used in the source. */
  @Column(name = "drug_source_concept_id")
  private Integer drugSourceConceptId;

  /** The information about the route of administration as detailed in the source. */
  @Column(name = "route_source_value")
  private String routeSourceValue;

  /** The information about the dose unit as detailed in the source. */
  @Column(name = "dose_unit_source_value")
  private String doseUnitSourceValue;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
