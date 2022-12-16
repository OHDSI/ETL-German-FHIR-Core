package org.miracum.etl.fhirtoomop.model;

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

/**
 * The PostProcessMap class describes the structure of the post_process_map table in OMOP CDM. The
 * post_process_map table contains data from FHIR resources which is to be transformed to OMOP CDM
 * through post processing using SQL scripts.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "post_process_map", schema = "cds_etl_helper")
public class PostProcessMap {
  /** A unique identifier for each record in post_process_map table. */
  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "data_id")
  private Long dataId;

  /** The resource type of the FHIR resource. */
  private String type;

  /** The source data one which is to be transformed to OMOP CDM. */
  @Column(name = "data_one")
  private String dataOne;

  /** The source data two which is to be transformed to OMOP CDM. */
  @Column(name = "data_two")
  private String dataTwo;

  /** A foreign key to a record in OMOP CDM. */
  @Column(name = "omop_id")
  private Long omopId;

  /** The metadata needed for mapping the data to OMOP CDM. */
  @Column(name = "omop_table")
  private String omopTable;

  /** The logical id of the FHIR resource. */
  @Column(name = "fhir_logical_id", nullable = true)
  private String fhirLogicalId;

  /** The identifier for the source data in the FHIR resource. */
  @Column(name = "fhir_identifier", nullable = true)
  private String fhirIdentifier;
}
