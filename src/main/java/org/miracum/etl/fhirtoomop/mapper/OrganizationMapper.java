package org.miracum.etl.fhirtoomop.mapper;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.apache.bcel.classfile.Module;
import org.hl7.fhir.r4.model.Organization;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.FhirMapper;
import org.miracum.etl.fhirtoomop.mapper.helpers.FindOmopConcepts;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceCheckDataAbsentReason;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.omop.CareSite;
import org.miracum.etl.fhirtoomop.model.omop.Location;
import org.miracum.etl.fhirtoomop.repository.service.PatientMapperServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.Random;

@Slf4j
@Component
public class OrganizationMapper implements FhirMapper<Organization> {

    private static final FhirSystems fhirSystems = new FhirSystems();
    private final IFhirPath fhirPath;
    private final Boolean bulkload;
    private final DbMappings dbMappings;

    @Autowired
    ResourceOmopReferenceUtils omopReferenceUtils;
    @Autowired
    ResourceFhirReferenceUtils fhirReferenceUtils;
    @Autowired
    PatientMapperServiceImpl patientService;
    @Autowired
    ResourceCheckDataAbsentReason checkDataAbsentReason;
    @Autowired
    FindOmopConcepts findOmopConcepts;

    private static final Counter noFhirReferenceCounter =
            MapperMetrics.setNoFhirReferenceCounter("stepProcessOrganization");
    private static final Counter deletedFhirReferenceCounter =
            MapperMetrics.setDeletedFhirRessourceCounter("stepProcessOrganization");

    /**
     * Constructor for objects of the class OrganizationMapper.
     *
     * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
     * @param bulkload parameter which indicates whether the Job should be run as bulk load or
     *     incremental load
     * @param dbMappings collections for the intermediate storage of data from OMOP CDM in RAM
     */
    @Autowired
    public OrganizationMapper(IFhirPath fhirPath, Boolean bulkload, DbMappings dbMappings) {

        this.fhirPath = fhirPath;
        this.bulkload = bulkload;
        this.dbMappings = dbMappings;
    }

    @Override
    public OmopModelWrapper map(Organization srcOrganization, boolean isDeleted) {
        var wrapper = new OmopModelWrapper();

        var organizationIdentifier = fhirReferenceUtils.extractIdentifier(srcOrganization,"MR");
        var organizationLogicId = fhirReferenceUtils.extractId(srcOrganization);
        if (Strings.isNullOrEmpty(organizationLogicId) && Strings.isNullOrEmpty(organizationIdentifier)) {
            log.warn("No [Identifier] or [Id] found. [Organization] resource is invalid. Skip resource");
            noFhirReferenceCounter.increment();
            return null;
        }

        String organizationId = "";
        if(!Strings.isNullOrEmpty(organizationId)){
            organizationId = srcOrganization.getId();
        }

        var organizationName = srcOrganization.getName();


        var organizationTag = srcOrganization.getMeta().getTag().stream().findFirst();
        if (organizationTag.isEmpty()) {
            return null;
        }
        var organizationMetaCoding =organizationTag.get();
        var concept = findOmopConcepts.getConcepts(organizationMetaCoding, null,bulkload,dbMappings,organizationId);
        var placeOfService = concept.getConceptId();
        int generatedLong = new Random().nextInt();

        var newCareSite = CareSite.builder()
                        .careSiteId((long) generatedLong).
                careSiteName(organizationName).
                placeOfServiceConceptId(placeOfService).build();
        wrapper.setCareSite(newCareSite);
        return wrapper;
    }
}
