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
 * The VisitDetail class describes the structure of the visit_detail table in OMOP CDM. The
 * visit_detail table is an optional table used to represents details of each record in the parent
 * visit_occurrence table.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@Builder
@Entity
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "visit_detail")
public class VisitDetail {
  /** A unique identifier for each Persons's visit or encounter at a healthcare provider. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  /** A foreign key identifier to the Person for whom the visit is recorded. */
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key that refers to a visit Concept Identifier in the Standardized Vocabularies
   * belonging to the Visit Vocabulary.
   */
  @Column(name = "visit_detail_concept_id")
  private Integer visitDetailConceptId;

  /** The start date of the visit. */
  @Column(name = "visit_detail_start_date")
  private LocalDate visitDetailStartDate;

  /** The date and time of the visit started. */
  @Column(name = "visit_detail_start_datetime")
  private LocalDateTime visitDetailStartDatetime;

  /** The end date of the visit. */
  @Column(name = "visit_detail_end_date")
  private LocalDate visitDetailEndDate;

  /** The date and time of the visit ended. */
  @Column(name = "visit_detail_end_datetime")
  private LocalDateTime visitDetailEndDatetime;

  /**
   * A foreign key to the predefined concept identifier in the Standardized Vocabularies reflecting
   * the type of source data from which the visit record is derived belonging to the Visit Type
   * vocabulary.
   */
  @Column(name = "visit_detail_type_concept_id")
  private Integer visitDetailTypeConceptId;

  /** A foreign key to the provider in the provider table who was associated with the visit. */
  @Column(name = "provider_id")
  private Integer providerId;

  /** A foreign key to the care site in the care site table that was visited. */
  @Column(name = "care_site_id")
  private Long careSiteId;

  /**
   * A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the
   * admitting source for a visit.
   */
  @Column(name = "admitting_source_concept_id")
  private Long admittingSourceConceptId;

  /**
   * A foreign key to the predefined concept in the Place of Service Vocabulary reflecting the
   * discharge disposition for a visit.
   */
  @Column(name = "discharge_to_concept_id")
  private Long dischargeToConceptId;

  /** A foreign key to the visit_detail table of the visit immediately preceding this visit. */
  @Column(name = "preceding_visit_detail_id")
  private Long precedingVisitDetailId;

  /** The source code for the visit as it appears in the source data. */
  @Column(name = "visit_detail_source_value")
  private String visitDetailSourceValue;

  /** A foreign key to a concept that refers to the code used in the source. */
  @Column(name = "visit_detail_source_concept_id")
  private Integer visitDetailSourceConceptId;

  /** The source code for where the patient was admitted from as it appears in the source data. */
  @Column(name = "admitting_source_value")
  private String admittingSourceValue;

  /** The source code for the discharge disposition as it appears in the source data. */
  @Column(name = "discharge_to_source_value")
  private String dischargeToSourceValue;

  /**
   * A foreign key to the visit_detail table record to represent the immediate parent visit-detail
   * record.
   */
  @Column(name = "visit_detail_parent_id")
  private Integer visitDetailParentId;

  /** A foreign key that refers to the record in the visit_occurrence table. */
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
