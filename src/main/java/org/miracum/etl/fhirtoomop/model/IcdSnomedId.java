package org.miracum.etl.fhirtoomop.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The IcdSnomedId class specifies a composite primary key class for icd_snomed_domain_lookup view
 * in OMOP CDM. The class is mapped to multiple fields of the icd_snomed_domain_lookup entity.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@SuppressWarnings("serial")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class IcdSnomedId implements Serializable {

  /** A foreign key to a Condition Concept that refers to the ICD-10-GM code. */
  private int icdGmConceptId;

  /** A foreign key to a Condition Concept that refers to the corresponding SNOMED Concept. */
  private int snomedConceptId;
}
