package org.miracum.etl.fhirtoomop.model.omop;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "device_exposure")
public class DeviceExposure {
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "device_exposure_id")
  private Long deviceExposureId;

  @Column(name = "person_id")
  private Long personId;

  @Column(name = "device_concept_id")
  private int deviceConceptId;

  @Column(name = "device_exposure_start_date")
  private LocalDate deviceExposureStartDate;

  @Column(name = "device_exposure_start_datetime")
  private LocalDateTime deviceExposureStartDatetime;

  @Column(name = "device_exposure_end_date")
  private LocalDate deviceExposureEndDate;

  @Column(name = "device_exposure_end_datetime")
  private LocalDateTime deviceExposureEndDatetime;

  @Column(name = "device_type_concept_id")
  private int deviceTypeConceptId;

  @Column(name = "unique_device_id")
  private Long uniqueDeviceId;

  @Column(name = "quantity")
  private BigDecimal quantity;

  @Column(name = "provider_id")
  private Long provicerId;

  @Column(name = "visit_occurrence_id")
  private Long visitOccurrenceId;

  @Column(name = "visit_detail_id")
  private Long visitDetailId;

  @Column(name = "device_source_value")
  private String deviceSourceValue;

  @Column(name = "device_source_concept_id")
  private int deviceSourceConceptId;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
