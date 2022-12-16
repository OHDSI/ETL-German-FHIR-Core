package org.miracum.etl.fhirtoomop.model;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The UniqueIcdCode class the assignment of a diagnosis and its period in relation to a person in
 * order to avoid duplicates in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class UniqueIcdCode {
  /** A cleaned ICD-10-GM code. */
  private String icdGmCode;

  /** A raw ICD-10-GM code as it appears in the source data. */
  private String icdGmSourceCode;

  /** A foreign key identifier to the Person who is experiencing the condition. */
  private Long personId;

  /** The date and time when the instance of the Condition is recorded. */
  private LocalDateTime startDateTime;

  /** The date and time when the instance of the Condition is considered to have ended. */
  private LocalDateTime endDateTime;

  /** A foreign key to a Condition Concept that refers to the corresponding SNOMED Concept. */
  private Integer snomedConceptId;
}
