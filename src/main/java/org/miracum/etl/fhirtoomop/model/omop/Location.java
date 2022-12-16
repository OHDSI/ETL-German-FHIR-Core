package org.miracum.etl.fhirtoomop.model.omop;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The Location class describes the structure of the location table in OMOP CDM. The location table
 * represents a generic way to capture physical location or address information of Persons and Care
 * Sites.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class Location {
  /** A unique identifier for each geographic location. */
  @Id
  @Column(name = "location_id")
  private Long locationId;

  /**
   * The address field 1, typically used for the street address, as it appears in the source data.
   */
  private String address_1;

  /**
   * The address field 2, typically used for additional detail such as buildings, suites, doors, as
   * it appears in the source data.
   */
  private String address_2;

  /** The city field as it appears in the source data. */
  private String city;

  /** The state field as it appears in the source data. */
  private String state;

  /** The zip or postal code. */
  private String zip;

  /** The county. */
  private String county;

  /**
   * The verbatim information that is used to uniquely identify the location as it appears in the
   * source data.
   */
  @Column(name = "location_source_value")
  private String locationSourceValue;
}
