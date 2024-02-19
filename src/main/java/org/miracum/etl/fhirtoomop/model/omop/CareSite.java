package org.miracum.etl.fhirtoomop.model.omop;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The CareSite class describes the structure of the care_site table in OMOP CDM. The care_site
 * table contains a list of uniquely identified institutional (physical or organizational) units
 * where healthcare delivery is practiced (offices, wards, hospitals, clinics, etc.).
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "care_site")
public class CareSite {
  /** A unique identifier for each Care Site. */
  @Id
  @Column(name = "care_site_id")
  private Long careSiteId;

  /** The verbatim description or name of the Care Site as in data source. */
  @Column(name = "care_site_name")
  private String careSiteName;

  /**
   * A foreign key that refers to a Place of Service Concept ID in the Standardized Vocabularies.
   */
  @Column(name = "place_of_service_concept_id")
  private Integer placeOfServiceConceptId;

  /**
   * A foreign key to the geographic Location in the location table, where the detailed address
   * information is stored.
   */
  @Column(name = "location_id")
  private Integer locationId;

  /** The identifier for the Care Site in the source data. */
  @Column(name = "care_site_source_value")
  private String careSiteSourceValue;

  /** The source code for the Place of Service as it appears in the source data. */
  @Column(name = "place_of_service_source_value")
  private String placeOfServiceSourceValue;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
