package org.miracum.etl.fhirtoomop.model.omop;

import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The Person class describes the structure of the person table in OMOP CDM. The Person Domain
 * contains records that uniquely identify each patient in the source data who is time at-risk to
 * have clinical observations recorded within the source systems.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class Person {
  /** A unique identifier for each person. */
  @Id
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key that refers to an identifier in the concept table for the unique gender of the
   * person.
   */
  @Column(name = "gender_concept_id")
  private Integer genderConceptId;

  /** The year of birth of the person. */
  @Column(name = "year_of_birth")
  private Integer yearOfBirth;

  /** The month of birth of the person. */
  @Column(name = "month_of_birth")
  private Integer monthOfBirth;

  /** The day of the month of birth of the person. */
  @Column(name = "day_of_birth")
  private Integer dayOfBirth;

  /** The date and time of birth of the person. */
  @Column(name = "birth_datetime")
  private LocalDateTime birthDatetime;

  /**
   * A foreign key that refers to an identifier in the concept table for the unique race of the
   * person.
   */
  @Column(name = "race_concept_id")
  private Integer raceConceptId;

  /**
   * A foreign key that refers to the standard concept identifier in the Standardized Vocabularies
   * for the ethnicity of the person.
   */
  @Column(name = "ethnicity_concept_id")
  private Integer ethnicityConceptId;

  /**
   * A foreign key to the place of residency for the person in the location table, where the
   * detailed address information is stored.
   */
  @Column(name = "location_id")
  private Long locationId;

  /** A foreign key to the primary care provider the person is seeing in the provider table. */
  @Column(name = "provider_id")
  private Integer providerId;

  /**
   * A foreign key to the site of primary care in the care_site table, where the details of the care
   * site are stored.
   */
  @Column(name = "care_site_id")
  private Integer careSiteId;

  /** An (encrypted) key derived from the person identifier in the source data. */
  @Column(name = "person_source_value")
  private String personSourceValue;

  /** The source code for the gender of the person as it appears in the source data. */
  @Column(name = "gender_source_value")
  private String genderSourceValue;

  /** A foreign key to the gender concept that refers to the code used in the source. */
  @Column(name = "gender_source_concept_id")
  private Integer genderSourceConceptId;

  /** The source code for the race of the person as it appears in the source data. */
  @Column(name = "race_source_value")
  private String raceSourceValue;

  /** A foreign key to the race concept that refers to the code used in the source. */
  @Column(name = "race_source_concept_id")
  private Integer raceSourceConceptId;

  /** The source code for the ethnicity of the person as it appears in the source data. */
  @Column(name = "ethnicity_source_value")
  private String ethnicitySourceValue;

  /** A foreign key to the ethnicity concept that refers to the code used in the source. */
  @Column(name = "ethnicity_source_concept_id")
  private Integer ethnicitySourceConceptId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
