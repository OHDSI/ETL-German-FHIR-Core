package org.miracum.etl.fhirtoomop.model.omop;

import java.time.LocalDate;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The Concept class describes the structure of the concept table in OMOP CDM. The Standardized
 * Vocabularies contains records, or Concepts, that uniquely identify each fundamental unit of
 * meaning used to express clinical information in all domain tables of the CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@Builder
@Entity
@AllArgsConstructor
@NoArgsConstructor
public class Concept {
  /** A unique identifier for each Concept across all domains. */
  @Id
  @Column(name = "concept_id")
  private int conceptId;

  /** An unambiguous, meaningful and descriptive name for the Concept. */
  @Column(name = "concept_name")
  private String conceptName;

  /** A foreign key to the domain table the Concept belongs to. */
  @Column(name = "domain_id")
  private String domainId;

  /**
   * A foreign key to the vocabulary table indicating from which source the Concept has been
   * adapted.
   */
  @Column(name = "vocabulary_id")
  private String vocabularyId;

  /** The attribute or concept class of the Concept. */
  @Column(name = "concept_class_id")
  private String conceptClassId;

  /** The concept code represents the identifier of the Concept in the source vocabulary. */
  @Column(name = "concept_code")
  private String conceptCode;

  /** The date when the Concept was first recorded. */
  @Column(name = "valid_start_date")
  private LocalDate validStartDate;

  /**
   * The date when the Concept became invalid because it was deleted or superseded (updated) by a
   * new concept.
   */
  @Column(name = "valid_end_date")
  private LocalDate validEndDate;

  public enum Domain {
    MEASUREMENT("Measurement"),
    CONDITION("Condition"),
    OBSERVATION("Observation");

    private final String label;

    Domain(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }

  public enum Vocabulary {
    ICD10GM("ICD10GM"),
    LOINC("LOINC");

    private final String label;

    Vocabulary(String label) {
      this.label = label;
    }

    public String getLabel() {
      return label;
    }
  }
}
