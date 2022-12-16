package org.miracum.etl.fhirtoomop.model.omop;

import java.time.LocalDate;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.miracum.etl.fhirtoomop.model.SourceToConceptId;

/**
 * The SourceToConceptMap class describes the structure of the source_to_concept_map table in OMOP
 * CDM. The source to concept map table is a legacy data structure within the OMOP Common Data
 * Model, recommended for use in ETL processes to maintain local source codes which are not
 * available as Concepts in the Standardized Vocabularies, and to establish mappings for each source
 * code into a Standard Concept as target_concept_ids that can be used to populate the Common Data
 * Model tables.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "source_to_concept_map")
@IdClass(SourceToConceptId.class)
public class SourceToConceptMap {
  /** The source code being translated into a Standard Concept. */
  @Id
  @Column(name = "source_code")
  private String sourceCode;

  /** A foreign key to the Source Concept that is being translated into a Standard Concept. */
  @Column(name = "source_concept_id")
  private Integer sourceConceptId;

  /**
   * A foreign key to the vocabulary table defining the vocabulary of the source code that is being
   * translated to a Standard Concept.
   */
  @Id
  @Column(name = "source_vocabulary_id")
  private String sourceVocabularyId;

  /** An optional description for the source code. */
  @Column(name = "source_code_description")
  private String sourceCodeDescription;

  /** A foreign key to the target Concept to which the source code is being mapped. */
  @Id
  @Column(name = "target_concept_id")
  private Integer targetConceptId;

  /** A foreign key to the vocabulary table defining the vocabulary of the target Concept. */
  @Column(name = "target_vocabulary_id")
  private String targetVocabularyId;

  /** The date when the mapping instance was first recorded. */
  @Column(name = "valid_start_date")
  private LocalDate validStartDate;

  /**
   * The date when the mapping instance became invalid because it was deleted or superseded
   * (updated) by a new relationship.
   */
  @Id
  @Column(name = "valid_end_date")
  private LocalDate validEndDate;

  /** Reason the mapping instance was invalidated. */
  @Column(name = "invalid_reason")
  private String invalidReason;
}
