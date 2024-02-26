package org.miracum.etl.fhirtoomop.mapper;

import ca.uhn.fhir.fhirpath.IFhirPath;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.Counter;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Enumerations;
import org.hl7.fhir.r4.model.PractitionerRole;
import org.hl7.fhir.r4.model.ResourceType;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.FhirMapper;
import org.miracum.etl.fhirtoomop.mapper.helpers.MapperMetrics;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class PractitionerRoleMapper implements FhirMapper<PractitionerRole> {

    private static final FhirSystems fhirSystems = new FhirSystems();

    private final IFhirPath fhirPath;
    private final Boolean bulkload;
    private final DbMappings dbMappings;

    @Autowired
    ResourceFhirReferenceUtils fhirReferenceUtils;

    private static final Counter noFhirReferenceCounter = MapperMetrics.setNoFhirReferenceCounter("stepProcessPractitionerRoles");


    public PractitionerRoleMapper(IFhirPath fhirPath, Boolean bulkload, DbMappings dbMappings) {
        this.fhirPath = fhirPath;
        this.bulkload = bulkload;
        this.dbMappings = dbMappings;
    }

    @Override
    public OmopModelWrapper map(PractitionerRole practitionerRole, boolean isDeleted) {
        var wrapper = new OmopModelWrapper();

        var practitionerRoleLogicalId = fhirReferenceUtils.extractId(practitionerRole);
        if (Strings.isNullOrEmpty(practitionerRoleLogicalId)) {
            log.warn("No [Identifier] or [Id] found. [PractitionerRole] resource is invalid. Skip resource");
            noFhirReferenceCounter.increment();
            return null;
        }

        practitionerRole.getPractitioner().getReferenceElement().getIdPart();

        String practitionerId = practitionerRole.getPractitioner().getReferenceElement().getIdPart();
        String organizationId = practitionerRole.getOrganization().getReferenceElement().getIdPart();

        log.warn("practitionerID " + practitionerId );
        log.warn("organizationId " + organizationId );


        String practitionerLogicalId = fhirReferenceUtils.extractId(ResourceType.Practitioner.name(), practitionerId);
        String organizationLogicalId = fhirReferenceUtils.extractId(ResourceType.Organization.name(), organizationId);

//        String practitionerId = practitionerRole.getPractitioner().getReference();
//        String organizationId = practitionerRole.getOrganization().getReference();

        if (practitionerLogicalId == null || organizationLogicalId == null) {
            log.warn("Either [practitionerId] or [organizationId] not found for [PractitionerRole] {}. Skip Resource", practitionerRoleLogicalId);
            if (bulkload.equals(Boolean.FALSE)) {
                deleteExistingPractitionerRoles(practitionerRoleLogicalId);
            }
        }

        var practitionerOrganizationReference = setPractitionerOrganizationReference(practitionerRoleLogicalId, practitionerLogicalId, organizationLogicalId);
        wrapper.getPostProcessMap().add(practitionerOrganizationReference);

        return wrapper;
    }

    private PostProcessMap setPractitionerOrganizationReference(String practitionerRoleLogicalId, String practitionerId, String organizationId) {
        return PostProcessMap.builder()
                .dataOne(practitionerId)
                .dataTwo(organizationId)
                .type(Enumerations.ResourceType.PRACTITIONERROLE.name())
                .fhirLogicalId(practitionerRoleLogicalId)
                .build();
    }

    private void deleteExistingPractitionerRoles(String practitionerRoleId) {
//        if (!Strings.isNullOrEmpty(practitionerRoleId)) {
//            patientService.deletePersonByFhirLogicalId(practitionerRoleId);
//        } else {
//            patientService.deletePersonByFhirIdentifier(patientSourceIdentifier);
//        }
    }
}
