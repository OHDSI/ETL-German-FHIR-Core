package org.miracum.etl.fhirtoomop.model.omop;

import java.math.BigDecimal;
import java.time.LocalDate;
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
 * The Measurement class describes the structure of the measurement table in OMOP CDM. The
 * measurement table contains records of Measurement, i.e. structured values (numerical or
 * categorical) obtained through systematic and standardized examination or testing of a Person or
 * Person's sample.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
public class Measurement {

  /** A unique identifier for each Measurement. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "measurement_id")
  private Long measurementId;

  /** A foreign key identifier to the Person about whom the measurement was recorded. */
  @Column(name = "person_id")
  private Long personId;

  /**
   * A foreign key to the standard measurement concept identifier in the Standardized Vocabularies.
   */
  @Column(name = "measurement_concept_id")
  private int measurementConceptId;

  /** The date of the Measurement. */
  @Column(name = "measurement_date")
  private LocalDate measurementDate;

  /** The date and time of the Measurement. */
  @Column(name = "measurement_datetime")
  private LocalDateTime measurementDatetime;

  /**
   * A foreign key to the predefined Concept in the Standardized Vocabularies reflecting the
   * provenance from where the Measurement record was recorded.
   */
  @Column(name = "measurement_type_concept_id")
  private int measurementTypeConceptId;

  /**
   * A foreign key identifier to the predefined Concept in the Standardized Vocabularies reflecting
   * the mathematical operator that is applied to the value_as_number.
   */
  @Column(name = "operator_concept_id")
  private Integer operatorConceptId;

  /** A Measurement result where the result is expressed as a numeric value. */
  @Column(name = "value_as_number")
  private BigDecimal valueAsNumber;

  /**
   * A foreign key to a Measurement result represented as a Concept from the Standardized
   * Vocabularies (e.g., positive/negative, present/absent, low/high, etc.).
   */
  @Column(name = "value_as_concept_id")
  private Integer valueAsConceptId;

  /**
   * A foreign key to a Standard Concept ID of Measurement Units in the Standardized Vocabularies.
   */
  @Column(name = "unit_concept_id")
  private Integer unitConceptId;

  /** The lower limit of the normal range of the Measurement result. */
  @Column(name = "range_low")
  private BigDecimal rangeLow;

  /** The upper limit of the normal range of the Measurement. */
  @Column(name = "range_high")
  private BigDecimal rangeHigh;

  /**
   * A foreign key to the provider in the provider table who was responsible for initiating or
   * obtaining the measurement.
   */
  @Column(name = "provider_id")
  private Integer providerId;

  /**
   * A foreign key to the Visit in the visit_occurrence table during which the Measurement was
   * recorded
   */
  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  /** The Measurement name as it appears in the source data. */
  @Column(name = "measurement_source_value")
  private String measurementSourceValue;

  /**
   * A foreign key to a Concept in the Standard Vocabularies that refers to the code used in the
   * source.
   */
  @Column(name = "measurement_source_concept_id")
  private Integer measurementSourceConceptId;

  /** The source code for the unit as it appears in the source data. */
  @Column(name = "unit_source_value")
  private String unitSourceValue;

  /**
   * The source value associated with the content of the value_as_number or value_as_concept_id as
   * stored in the source data.
   */
  @Column(name = "value_source_value")
  private String valueSourceValue;

  /** The visit_detail record during which the Measurement occurred. */
  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
