package org.miracum.etl.fhirtoomop.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The FhirPsqlResource class describes the structure of the FHIR Gateway. The FHIR Gateway contains
 * FHIR resources in JSON format and their related metadata.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class FhirPsqlResource {
  /** A unique identifier for each FHIR resource in FHIR Gateway. */
  private String id;

  /** The logical id of the FHIR resource. */
  private String fhirId;

  /** The resource type of the FHIR resource. */
  private String type;

  /** The FHIR resource in JSON format. */
  private String data;

  /** The flag for resource, whether the resource is valid */
  private Boolean isDeleted;
}
