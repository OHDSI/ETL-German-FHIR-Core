package org.miracum.etl.fhirtoomop.model.omop;

import java.time.LocalDate;
import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The Death class describes the structure of the death table in OMOP CDM. The death domain contains
 * the clinical event for how and when a Person dies.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "death")
public class Death {
  /** A foreign key identifier to the deceased person. */
  @Id
  @Column(name = "person_id")
  private Long personId;

  /** The date the person was deceased. */
  @Column(name = "death_date")
  private LocalDate deathDate;

  /** The date and time the person was deceased. */
  @Column(name = "death_datetime")
  private LocalDateTime deathDatetime;

  /**
   * A foreign key referring to the predefined concept identifier in the Standardized Vocabularies
   * reflecting how the death was represented in the source data.
   */
  @Column(name = "death_type_concept_id")
  private int deathTypeConceptId;

  /**
   * A foreign key referring to a standard concept identifier in the Standardized Vocabularies for
   * conditions.
   */
  @Column(name = "cause_concept_id")
  private Integer causeConceptId;

  /** The source code for the cause of death as it appears in the source data. */
  @Column(name = "cause_source_value")
  private String causeSourceValue;

  /** A foreign key to the concept that refers to the code used in the source. */
  @Column(name = "cause_source_concept_id")
  private Integer causeSourceConceptId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
