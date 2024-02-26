package org.miracum.etl.fhirtoomop.model.omop;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * The specimen domain contains the records identifying biological samples from a person.
 *
 * @author Nitin Sabale
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
public class Specimen {
  /** A unique identifier for each person. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "specimen_id")
  private Long specimenId;

  @Column(name = "person_id")
  private Long personId;

  @Column(name = "specimen_concept_id")
  private Integer specimenConceptId;

  @Column(name = "specimen_type_concept_id")
  private Integer specimenTypeConceptId;

  @Column(name = "specimen_date")
  private LocalDate specimenDate;

  @Column(name = "specimen_datetime")
  private LocalDateTime specimenDateTime;

  @Column(name = "quantity")
  private Float quantity;

  @Column(name = "unit_concept_id")
  private Long unitConceptId;

  @Column(name = "anatomic_site_concept_id")
  private Long anatomicSiteConceptId;

  @Column(name = "disease_status_concept_id")
  private Long diseaseStatusConceptId;

  @Column(name = "specimen_source_id")
  private String specimenSourceId;

  @Column(name = "specimen_source_value")
  private String specimenSourceValue;

  @Column(name = "unit_source_value")
  private String unitSourceValue;

  @Column(name = "anatomic_site_source_value")
  private String anatomicSiteSourceValue;

  @Column(name = "disease_status_source_value")
  private String diseaseStatusSourceValue;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
