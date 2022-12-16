package org.miracum.etl.fhirtoomop.model.omop;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.miracum.etl.fhirtoomop.model.FactRelationshipId;

/**
 * The FactRelationship class describes the structure of the fact_relationship table in OMOP CDM.
 * The fact_relationship table contains records about the relationships between facts stored as
 * records in any table of the CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@IdClass(FactRelationshipId.class)
@Table(name = "fact_relationship")
public class FactRelationship {
  /**
   * The concept representing the domain of fact one, from which the corresponding table can be
   * inferred.
   */
  @Id
  @Column(name = "domain_concept_id_1")
  private int domainConceptId1;

  /** The unique identifier in the table corresponding to the domain of fact one. */
  @Id
  @Column(name = "fact_id_1")
  private Long factId1;

  /**
   * The concept representing the domain of fact two, from which the corresponding table can be
   * inferred.
   */
  @Id
  @Column(name = "domain_concept_id_2")
  private int domainConceptId2;

  /** The unique identifier in the table corresponding to the domain of fact two. */
  @Id
  @Column(name = "fact_id_2")
  private Long factId2;

  /** A foreign key to a Standard Concept ID of relationship in the Standardized Vocabularies. */
  @Column(name = "relationship_concept_id")
  private int relationshipConceptId;

  /** The logical id of the first FHIR resource. */
  @Column(name = "fhir_logical_id_1", nullable = true)
  private String fhirLogicalId1;

  /** The identifier for the source data in the first FHIR resource. */
  @Column(name = "fhir_identifier_1", nullable = true)
  private String fhirIdentifier1;

  /** The logical id of the second FHIR resource. */
  @Column(name = "fhir_logical_id_2", nullable = true)
  private String fhirLogicalId2;

  /** The identifier for the source data in the second FHIR resource. */
  @Column(name = "fhir_identifier_2", nullable = true)
  private String fhirIdentifier2;
}
