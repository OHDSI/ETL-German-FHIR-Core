package org.miracum.etl.fhirtoomop.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The LoincStandardId class specifies a composite primary key class for
 * loinc_standard_domain_lookup view in OMOP CDM. The class is mapped to multiple fields of the
 * loinc_standard_domain_lookup entity.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@SuppressWarnings("serial")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class LoincStandardId implements Serializable {

  /** A foreign key to a concept that refers to the LOINC code. */
  private int sourceConceptId;

  /** A foreign key to a concept that refers to the corresponding standard concept. */
  private int standardConceptId;
}
