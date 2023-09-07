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
 * The OrphaSnomedMapping class describes the structure of the orpha_snomed_mapping table in OMOP
 * CDM. The orpha_snomed_mapping table contains the mapping of Orpha codes to SNOMED codes.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@IdClass(OrphaSnomedId.class)
@Table(name = "orpha_snomed_mapping", schema = "cds_etl_helper")
public class OrphaSnomedMapping {

  /** The Orpha code which has a mapping to SNOMED. */
  @Id
  @Column(name = "orpha_code")
  private String orphaCode;

  /** A foreign key to a Condition Concept that refers to the Orpha code. */
  @Column(name = "orpha_concept_id")
  private int orphaConceptId;

  /** The valid start date of the Orpha code. */
  @Column(name = "orpha_valid_start_date")
  private LocalDate orphaValidStartDate;

  /** The valid end date of the Orpha code. */
  @Column(name = "orpha_valid_end_date")
  private LocalDate orphaValidEndDate;

  /** The SNOMED code to which Orpha codes are mapped. */
  @Id
  @Column(name = "snomed_code")
  private String snomedCode;

  /** A foreign key to a Condition Concept that refers to the corresponding SNOMED Concept. */
  @Id
  @Column(name = "snomed_concept_id")
  private int snomedConceptId;

  /** A foreign key to the domain table the SNOMED Concept belongs to. */
  @Column(name = "snomed_domain_id")
  private String snomedDomainId;

  /** A foreign key to a Domain Concept that refers to the corresponding SNOMED Domain. */
  @Column(name = "snomed_domain_concept_id")
  private int snomedDomainConceptId;

  /** The valid start date of the mapping between Orpha code and SNOMED code. */
  @Column(name = "mapping_valid_start_date")
  private LocalDate mappingValidStartDate;

  /** The valid end date of the mapping between Orpha code and SNOMED code. */
  @Column(name = "mapping_valid_end_date")
  private LocalDate mappingValidEndDate;
}
