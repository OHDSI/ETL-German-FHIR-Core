package org.miracum.etl.fhirtoomop.model.omop;

import java.time.LocalDate;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The ObservationPeriod class describes the structure of the observation_period table in OMOP CDM.
 * The observation_period table contains records which uniquely define the spans of time for which a
 * Person is at-risk to have clinical events recorded within the source systems, even if no events
 * in fact are recorded (healthy patient with no healthcare interactions).
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "observation_period")
public class ObservationPeriod {

  /** A unique identifier for each observation_period. */
  @Id
  @GeneratedValue
  @Column(name = "observation_period_id")
  private Long observationPeriodId;

  /** A foreign key identifier to the person for whom the observation period is defined. */
  @Column(name = "person_id")
  private Long personId;

  /** The start date of the observation period for which data are available from the data source. */
  @Column(name = "observation_period_start_date")
  private LocalDate observationPeriodStartDate;

  /** The end date of the observation period for which data are available from the data source. */
  @Column(name = "observation_period_end_date")
  private LocalDate observationPeriodEndDate;

  /**
   * A foreign key identifier to the predefined concept in the Standardized Vocabularies reflecting
   * the source of the observation period information.
   */
  @Column(name = "period_type_concept_id")
  private int periodTypeConceptId;
}
