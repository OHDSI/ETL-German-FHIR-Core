package org.miracum.etl.fhirtoomop.model;

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

/**
 * The LoincStandardDomainLookup class describes the structure of the loinc_standard_domain_lookup
 * view in OMOP CDM. The loinc_standard_domain_lookup view contains the mapping from LOINC to
 * standard concepts in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@IdClass(LoincStandardId.class)
@Table(name = "loinc_standard_domain_lookup", schema = "cds_etl_helper")
public class StandardDomainLookup {

  /** An LOINC code. */
  @Column(name = "source_code")
  private String sourceCode;

  /** A foreign key to a concept that refers to the LOINC code. */
  @Id
  @Column(name = "source_concept_id")
  private int sourceConceptId;

  /** A foreign key to a concept that refers to the corresponding standard concept. */
  @Id
  @Column(name = "standard_concept_id")
  private int standardConceptId;

  /** A foreign key to the domain table the standard concept belongs to. */
  @Column(name = "standard_domain_id")
  private String standardDomainId;

  /** A foreign key to a concept that refers to the domain of the standard concept. */
  @Column(name = "standard_domain_concept_id")
  private String standardDomainConceptId;

  /** The valid start date of the LOINC code. */
  @Column(name = "source_valid_start_date")
  private LocalDate sourceValidStartDate;

  /** The valid end date of the LOINC code. */
  @Column(name = "source_valid_end_date")
  private LocalDate sourceValidEndDate;

  /** The valid start date of the mapping between LOINC code and standard concept. */
  @Column(name = "mapping_valid_start_date")
  private LocalDate mappingValidStartDate;

  /** The valid end date of the mapping between LOINC code and standard concept. */
  @Column(name = "mapping_valid_end_date")
  private LocalDate mappingValidEndDate;
}
