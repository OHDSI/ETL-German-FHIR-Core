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
 * The ConditionOccurrence class describes the structure of the condition_occurrence table in OMOP
 * CDM. Conditions are records of a Person suggesting the presence of a disease or medical condition
 * stated as a diagnosis, a sign or a symptom, which is either observed by a Provider or reported by
 * the patient.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Builder
@Table(name = "condition_occurrence")
public class ConditionOccurrence {
  /** A unique identifier for each Condition Occurrence event. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "condition_occurrence_id")
  private Long conditionOccurrenceId;

  /** A foreign key identifier to the Person who is experiencing the condition. */
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key that refers to a Standard Condition Concept identifier in the Standardized
   * Vocabularies.
   */
  @Column(name = "condition_concept_id")
  private int conditionConceptId;

  /** The date when the instance of the Condition is recorded. */
  @Column(name = "condition_start_date")
  private LocalDate conditionStartDate;

  /** The date and time when the instance of the Condition is recorded. */
  @Column(name = "condition_start_datetime")
  private LocalDateTime conditionStartDatetime;

  /** The date when the instance of the Condition is considered to have ended. */
  @Column(name = "condition_end_date")
  private LocalDate conditionEndDate;

  /** The date and time when the instance of the Condition is considered to have ended. */
  @Column(name = "condition_end_datetime")
  private LocalDateTime conditionEndDatetime;

  /**
   * A foreign key to the predefined Concept identifier in the Standardized Vocabularies reflecting
   * the source data from which the condition was recorded, the level of standardization, and the
   * type of occurrence.
   */
  @Column(name = "condition_type_concept_id")
  private int conditionTypeConceptId;

  /** The reason that the condition was no longer present, as indicated in the source data. */
  @Column(name = "stop_reason")
  private String stopReason;

  /**
   * A foreign key to the Provider in the provider table who was responsible for capturing
   * (diagnosing) the Condition.
   */
  @Column(name = "provider_id")
  private Integer providerId;

  /**
   * A foreign key to the visit in the visit_occurrence table during which the Condition was
   * determined (diagnosed).
   */
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** The source code for the condition as it appears in the source data. */
  @Column(name = "condition_source_value")
  private String conditionSourceValue;

  /** A foreign key to a Condition Concept that refers to the code used in the source. */
  @Column(name = "condition_source_concept_id")
  private Integer conditionSourceConceptId;

  /** The source code for the condition status as it appears in the source data. */
  @Column(name = "condition_status_source_value")
  private String conditionStatusSourceValue;

  /**
   * A foreign key to the predefined concept in the standard vocabulary reflecting the condition
   * status
   */
  @Column(name = "condition_status_concept_id")
  private Integer conditionStatusConceptId;

  /** The visit_detail record during which the condition occurred. */
  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
