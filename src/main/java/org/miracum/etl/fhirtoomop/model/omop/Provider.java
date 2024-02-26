package org.miracum.etl.fhirtoomop.model.omop;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
/**
 * {@code Provider} represents a provider entity in the database.
 * This entity is mapped to the "provider" table in the "cds_cdm" schema.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "provider", schema = "cds_cdm")
public class Provider {
    /** The unique identifier of the provider. */
    @Id
    @Column(name = "provider_id")
    private Integer providerId;
    /** The name of the provider. */
    @Column(name = "provider_name")
    private String providerName;
    /** The National Provider Identifier (NPI) of the provider. */
    @Column(name = "npi")
    private String npi;
    /** The Drug Enforcement Administration (DEA) number of the provider. */
    @Column(name = "dea")
    private String dea;
    /** The concept ID representing the specialty of the provider. */
    @Column(name = "specialty_concept_id")
    private Integer specialtyConceptId;
    /** The ID of the care site associated with the provider. */
    @Column(name = "care_site_id")
    private Integer careSiteId;
    /** The year of birth of the provider. */
    @Column(name = "year_of_birth")
    private Integer yearOfBirth;
    /** The concept ID representing the gender of the provider. */
    @Column(name = "gender_concept_id")
    private Integer genderConceptId;
    /** The source value of the provider. */
    @Column(name = "provider_source_value")
    private String providerSourceValue;
    /** The source value of the specialty. */
    @Column(name = "specialty_source_value")
    private String specialtySourceValue;
    /** The concept ID representing the source of the specialty. */
    @Column(name = "specialty_source_concept_id")
    private Integer specialtySourceConceptId;
    /** The source value of the gender. */
    @Column(name = "gender_source_value")
    private String genderSourceValue;
    /** The concept ID representing the source of the gender. */
    @Column(name = "gender_source_concept_id")
    private Integer genderSourceConceptId;

    /** The logical id of the FHIR resource. */
    @Column(name = "fhir_logical_id", nullable = true)
    private String fhirLogicalId;


    // Constructors, getters, setters, etc.

    @Override
    public String toString() {
        return "Provider{" +
                "providerId=" + providerId +
                ", providerName='" + providerName + '\'' +
                ", npi='" + npi + '\'' +
                ", dea='" + dea + '\'' +
                ", specialtyConceptId=" + specialtyConceptId +
                ", careSiteId=" + careSiteId +
                ", yearOfBirth=" + yearOfBirth +
                ", genderConceptId=" + genderConceptId +
                ", providerSourceValue='" + providerSourceValue + '\'' +
                ", specialtySourceValue='" + specialtySourceValue + '\'' +
                ", specialtySourceConceptId=" + specialtySourceConceptId +
                ", genderSourceValue='" + genderSourceValue + '\'' +
                ", genderSourceConceptId=" + genderSourceConceptId +
                '}';
    }

}

