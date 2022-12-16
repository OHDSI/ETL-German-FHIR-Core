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
 * The OmopObservation class describes the structure of the observation table in OMOP CDM. The
 * observation table captures clinical facts about a Person obtained in the context of examination,
 * questioning or a procedure.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "observation")
public class OmopObservation {
  /** A unique identifier for each observation. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "observation_id")
  private Long observationId;

  /** A foreign key identifier to the Person about whom the observation was recorded. */
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key to the standard observation concept identifier in the Standardized Vocabularies.
   */
  @Column(name = "observation_concept_id")
  private int observationConceptId;

  /** The date of the observation. */
  @Column(name = "observation_date")
  private LocalDate observationDate;

  /** The date and time of the observation. */
  @Column(name = "observation_datetime")
  private LocalDateTime observationDatetime;

  /**
   * A foreign key to the predefined concept identifier in the Standardized Vocabularies reflecting
   * the type of the observation.
   */
  @Column(name = "observation_type_concept_id")
  private int observationTypeConceptId;

  /** The observation result stored as a number. */
  @Column(name = "value_as_number")
  private BigDecimal valueAsNumber;

  /** The observation result stored as a string. */
  @Column(name = "value_as_string")
  private String valueAsString;

  /** A foreign key to an observation result stored as a Concept ID. */
  @Column(name = "value_as_concept_id")
  private Integer valueAsConceptId;

  /** A foreign key to a Standard Concept ID for a qualifier. */
  @Column(name = "qualifier_concept_id")
  private Integer qualifierConceptId;

  /**
   * A foreign key to a Standard Concept ID of measurement units in the Standardized Vocabularies.
   */
  @Column(name = "unit_concept_id")
  private Integer unitConceptId;

  /**
   * A foreign key to the provider in the provider table who was responsible for making the
   * observation.
   */
  @Column(name = "provider_id")
  private Integer providerId;

  /**
   * A foreign key to the visit in the visit_occurrence table during which the observation was
   * recorded.
   */
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** The observation code as it appears in the source data. */
  @Column(name = "observation_source_value")
  private String observationSourceValue;

  /** A foreign key to a Concept that refers to the code used in the source. */
  @Column(name = "observation_source_concept_id")
  private Integer observationSourceConceptId;

  /** The source code for the unit as it appears in the source data. */
  @Column(name = "unit_source_value")
  private String unitSourceValue;

  /** The source value associated with a qualifier to characterize the observation. */
  @Column(name = "qualifier_source_value")
  private String qualifierSourceValue;

  /** The visit_detail record during which the Observation occurred. */
  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
