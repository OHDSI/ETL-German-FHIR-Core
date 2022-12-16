package org.miracum.etl.fhirtoomop.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Index;
import javax.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The MedicationIdMap class describes the structure of the medication_id_map table in OMOP CDM. The
 * medication_id_map table contains the FHIR IDs and the ATC code from FHIR Medication resources.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@Entity
@Table(
    name = "medication_id_map",
    schema = "cds_etl_helper",
    indexes = {
      @Index(
          name = "idx_fhir_logical_id_identifier_medication",
          columnList = "fhir_logical_id,fhir_identifier")
    })
public class MedicationIdMap {

  /** A unique identifier for each FHIR ID to OMOP ID reference. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "fhir_omop_id")
  private Long fhirOmopId;

  /** The resource type of the FHIR Medication resource. */
  private String type;

  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;

  /** The ATC code from the FHIR Medication resource. */
  @Column(name = "atc")
  private String atc;
}
