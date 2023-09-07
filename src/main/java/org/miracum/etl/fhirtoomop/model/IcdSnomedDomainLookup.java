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
 * The IcdSnomedDomainLookup class describes the structure of the icd_snomed_domain_lookup view in
 * OMOP CDM. The icd_snomed_domain_lookup view contains the mapping between ICD-10-GM and SNOMED.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@IdClass(IcdSnomedId.class)
@Table(name = "icd_snomed_domain_lookup", schema = "cds_etl_helper")
public class IcdSnomedDomainLookup {

  /** An ICD-10-GM code. */
  @Column(name = "icd_gm_code")
  private String icdGmCode;

  /** A foreign key to a Condition Concept that refers to the ICD-10-GM code. */
  @Id
  @Column(name = "icd_gm_concept_id")
  private int icdGmConceptId;

  /** A foreign key to a Condition Concept that refers to the corresponding SNOMED Concept. */
  @Id
  @Column(name = "snomed_concept_id")
  private int snomedConceptId;

  /** A foreign key to the domain table the SNOMED Concept belongs to. */
  @Column(name = "snomed_domain_id")
  private String snomedDomainId;

  /** The date when the ICD-10-GM Concept was first recorded. */
  @Column(name = "icd_gm_valid_start_date")
  private LocalDate icdGmValidStartDate;

  /**
   * The date when the ICD-10-GM Concept became invalid because it was deleted or superseded
   * (updated) by a new concept.
   */
  @Column(name = "icd_gm_valid_end_date")
  private LocalDate icdGmValidEndDate;
}
