package org.miracum.etl.fhirtoomop.mapper;

import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Practitioner;
import org.hl7.fhir.r4.model.StringType;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.Provider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * {@code PractitionerMapper} maps FHIR Practitioner resources to OMOP Provider entities.
 * This mapper implements the {@code FhirMapper} interface.
 */
@Slf4j
@Component
public class PractitionerMapper implements FhirMapper<Practitioner> {

    private final Boolean bulkload;
    private final DbMappings dbMappings;

    @Autowired
    ResourceFhirReferenceUtils fhirReferenceUtils;
    /**
     * Constructs a new {@code PractitionerMapper} with the specified bulkload flag and database mappings.
     *
     * @param bulkload   Indicates whether the mapper operates in bulk load mode.
     * @param dbMappings The database mappings used for mapping resources.
     */
    public PractitionerMapper(Boolean bulkload, DbMappings dbMappings) {
        this.bulkload = bulkload;
        this.dbMappings = dbMappings;
    }

    /**
     * Maps a FHIR Practitioner resource to an OMOP Model Wrapper.
     *
     * @param resource  The FHIR Practitioner resource to be mapped.
     * @param isDeleted A boolean indicating whether the resource is deleted.
     * @return An {@code OmopModelWrapper} containing the mapped OMOP entities.
     */
    @Override
    public OmopModelWrapper map(Practitioner resource, boolean isDeleted) {
        var wrapper = new OmopModelWrapper();
        Random rand = new Random();
        String gender = String.valueOf(resource.getGenderElement().getValue());
        Integer id = Math.abs(rand.nextInt());
        List<StringType> givenNames = resource.getName().get(0).getGiven();
        List<String> givenNamesStrings = givenNames.stream()
                .map(StringType::getValue)
                .collect(Collectors.toList());
        String concatenatedGivenNames = String.join(" ", givenNamesStrings);
        String familyName = resource.getName().get(0).getFamily();
        String fullName = concatenatedGivenNames + " " + familyName;

        Date date = resource.getBirthDate();
        LocalDate localDate;
        int year = 0;
        if (date != null) {
            localDate = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
            year = localDate.getYear();
        }
        var practitionerId = fhirReferenceUtils.extractId(resource);
        Provider provider = setUpProvider(gender, id, fullName, year != 0 ? year : null, practitionerId);
        wrapper.getProvider().add(provider);
        return wrapper;
    }
    /**
     * Sets up a Provider entity based on the provided parameters.
     *
     * @param gender         The gender of the provider.
     * @param id             The identifier of the provider.
     * @param fullName       The full name of the provider.
     * @param year           The year of birth of the provider (nullable).
     * @param practitionerId The logical identifier of the practitioner.
     * @return A {@code Provider} entity.
     */
    private Provider setUpProvider(String gender,Integer id,String fullName, Integer year, String practitionerId ){
        var provider =
                Provider.builder()
                        .providerId(id)
                        .providerName(fullName)
                        .npi("")
                        .dea("")
                        .specialtyConceptId(1)
                        .careSiteId(null)
                        .yearOfBirth(year)
                        .genderConceptId(null)
                        .providerSourceValue("")
                        .specialtySourceValue("")
                        .specialtySourceConceptId(null)
                        .genderSourceValue(gender)
                        .genderSourceConceptId(null)
                        .fhirLogicalId(practitionerId)
                        .build();
        return provider;
    }
}
