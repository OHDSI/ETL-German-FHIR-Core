package org.miracum.etl.fhirtoomop.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The FactRelationshipId class specifies a composite primary key class for fact_relationship table
 * in OMOP CDM. The class is mapped to multiple fields of the fact_relationship entity.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@SuppressWarnings("serial")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FactRelationshipId implements Serializable {

  /**
   * The concept representing the domain of fact one, from which the corresponding table can be
   * inferred.
   */
  private int domainConceptId1;

  /** The unique identifier in the table corresponding to the domain of fact one. */
  private Long factId1;

  /**
   * The concept representing the domain of fact two, from which the corresponding table can be
   * inferred.
   */
  private int domainConceptId2;

  /** The unique identifier in the table corresponding to the domain of fact two. */
  private Long factId2;
}
