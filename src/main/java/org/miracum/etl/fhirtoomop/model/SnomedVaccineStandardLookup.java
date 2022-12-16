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
 * The IcdSnomedDomainLookup class describes the structure of the icd_snomed_domain_lookup table in
 * OMOP CDM. The icd_snomed_domain_lookup table contains the mapping between ICD-10-GM and SNOMED.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@IdClass(SnomedVaccineId.class)
@Table(name = "snomed_vaccine_standard_lookup", schema = "cds_etl_helper")
public class SnomedVaccineStandardLookup {
  /** An SNOMED code. */
  @Column(name = "snomed_code")
  private String snomedCode;

  /** A foreign key to a Vaccine Concept that refers to the SNOMED code. */
  @Id
  @Column(name = "snomed_concept_id")
  private int snomedConceptId;

  /** A foreign key to a Vaccine Concept that refers to the corresponding standard Concept. */
  @Id
  @Column(name = "standard_vaccine_concept_id")
  private int standardVaccineConceptId;

  /** A foreign key to the domain table the standard Concept belongs to. */
  @Column(name = "standard_vaccine_domain_id")
  private String standardVaccineDomainId;

  /** The date when the SNOMED Concept was first recorded. */
  @Column(name = "snomed_valid_start_date")
  private LocalDate snomedValidStartDate;

  /**
   * The date when the SNOMED Concept became invalid because it was deleted or superseded (updated)
   * by a new concept.
   */
  @Column(name = "snomed_valid_end_date")
  private LocalDate snomedValidEndDate;
}
