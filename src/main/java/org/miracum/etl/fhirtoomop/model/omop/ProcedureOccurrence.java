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
 * The ProcedureOccurrence class describes the structure of the procedure_occurrence table in OMOP
 * CDM. The PROCEDURE_OCCURRENCE table contains records of activities or processes ordered by, or
 * carried out by, a healthcare provider on the patient to have a diagnostic or therapeutic purpose.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(name = "procedure_occurrence")
public class ProcedureOccurrence {
  /** A system-generated unique identifier for each Procedure Occurrence. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "procedure_occurrence_id")
  private Long procedureOccurrenceId;

  /** A foreign key identifier to the Person who is subjected to the Procedure. */
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key that refers to a standard procedure Concept identifier in the Standardized
   * Vocabularies.
   */
  @Column(name = "procedure_concept_id")
  private int procedureConceptId;

  /** The date on which the Procedure was performed. */
  @Column(name = "procedure_date")
  private LocalDate procedureDate;

  /** The date and time on which the Procedure was performed. */
  @Column(name = "procedure_datetime")
  private LocalDateTime procedureDatetime;

  /**
   * A foreign key to the predefined Concept identifier in the Standardized Vocabularies reflecting
   * the type of source data from which the procedure record is derived.
   */
  @Column(name = "procedure_type_concept_id")
  private int procedureTypeConceptId;

  /** A foreign key to a Standard Concept identifier for a modifier to the Procedure */
  @Column(name = "modifier_concept_id")
  private Integer modifierConceptId;

  /** The quantity of procedures ordered or administered. */
  private Integer quantity;

  /**
   * A foreign key to the provider in the provider table who was responsible for carrying out the
   * procedure.
   */
  @Column(name = "provider_id")
  private Integer providerId;

  /** A foreign key to the visit in the visit table during which the Procedure was carried out. */
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** The source code for the Procedure as it appears in the source data. */
  @Column(name = "procedure_source_value")
  private String procedureSourceValue;

  /** A foreign key to a Procedure Concept that refers to the code used in the source. */
  @Column(name = "procedure_source_concept_id")
  private Integer procedureSourceConceptId;

  /** The source code for the qualifier as it appears in the source data. */
  @Column(name = "modifier_source_value")
  private String modifierSourceValue;

  /** The visit_detail record during which the Procedure occurred. */
  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
