package org.miracum.etl.fhirtoomop.model.omop;

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
 * The VisitOccurrence class describes the structure of the visit_occurrence table in OMOP CDM. The
 * visit_occurrence table contains the spans of time a Person continuously receives medical services
 * from one or more providers at a Care Site in a given setting within the health care system.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "visit_occurrence")
public class VisitOccurrence {
  /** A unique identifier for each Person's visit or encounter at a healthcare provider. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** A foreign key identifier to the Person for whom the visit is recorded. */
  @Column(name = "person_id")
  private Long personId;

  /** A foreign key that refers to a visit Concept identifier in the Standardized Vocabularies. */
  @Column(name = "visit_concept_id")
  private int visitConceptId;

  /** The start date of the visit. */
  @Column(name = "visit_start_date")
  private LocalDate visitStartDate;

  /** The date and time of the visit started. */
  @Column(name = "visit_start_datetime")
  private LocalDateTime visitStartDatetime;

  /** The end date of the visit. */
  @Column(name = "visit_end_date")
  private LocalDate visitEndDate;

  /** The date and time of the visit end. */
  @Column(name = "visit_end_datetime")
  private LocalDateTime visitEndDatetime;

  /**
   * A foreign key to the predefined Concept identifier in the Standardized Vocabularies reflecting
   * the type of source data from which the visit record is derived.
   */
  @Column(name = "visit_type_concept_id")
  private Integer visitTypeConceptId;

  /** A foreign key to the provider in the provider table who was associated with the visit. */
  @Column(name = "provider_id")
  private Integer providerId;

  /** A foreign key to the care site in the care site table that was visited. */
  @Column(name = "care_site_id")
  private Integer careSiteId;

  /** The source code for the visit as it appears in the source data. */
  @Column(name = "visit_source_value")
  private String visitSourceValue;

  /** A foreign key to a Concept that refers to the code used in the source. */
  @Column(name = "visit_source_concept_id")
  private Integer visitSourceConceptId;

  /**
   * A foreign key to the predefined concept in the Place of Service Vocabulary reflecting where the
   * patient was admitted from.
   */
  @Column(name = "admitting_source_concept_id")
  private Integer admittingSourceConceptId;

  /** The source code for the admitting source as it appears in the source data. */
  @Column(name = "admitting_source_value")
  private String admittingSourceValue;

  /**
   * A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the
   * discharge disposition for a visit.
   */
  @Column(name = "discharge_to_concept_id")
  private Integer dischargeToConceptId;

  /** The source code for the discharge disposition as it appears in the source data. */
  @Column(name = "discharge_to_source_value")
  private String dischargeToSourceValue;

  /** A foreign key to the visit_occurrence table of the visit immediately preceding this visit. */
  @Column(name = "preceding_visit_occurrence_id")
  private Integer precedingVisitOccurrenceId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
