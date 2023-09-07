package org.miracum.etl.fhirtoomop.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The OrphaSnomedId class specifies a composite primary key class for orpha_snomed_mapping table in
 * OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@SuppressWarnings("serial")
@AllArgsConstructor
@NoArgsConstructor
@Data
public class OrphaSnomedId implements Serializable {

  /** The Orpha code which has a mapping to SNOMED. */
  private String orphaCode;

  /** The SNOMED code to which Orpha codes are mapped. */
  private String snomedCode;
}
