package org.miracum.etl.fhirtoomop.model;

import java.io.Serializable;
import java.time.LocalDate;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The SourceToConceptId class specifies a composite primary key class for source_to_concept_map
 * table in OMOP CDM. The class is mapped to multiple fields of the source_to_concept_map entity.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@SuppressWarnings("serial")
@NoArgsConstructor
@AllArgsConstructor
@Data
public class SourceToConceptId implements Serializable {
  /** The source code being translated into a Standard Concept. */
  private String sourceCode;

  /**
   * A foreign key to the vocabulary table defining the vocabulary of the source code that is being
   * translated to a Standard Concept.
   */
  private String sourceVocabularyId;

  /** A foreign key to the target Concept to which the source code is being mapped. */
  private Integer targetConceptId;

  /**
   * he date when the mapping instance became invalid because it was deleted or superseded (updated)
   * by a new relationship.
   */
  private LocalDate validEndDate;
}
