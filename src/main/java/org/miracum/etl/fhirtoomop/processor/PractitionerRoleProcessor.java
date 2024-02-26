package org.miracum.etl.fhirtoomop.processor;

import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.PractitionerRole;
import org.miracum.etl.fhirtoomop.mapper.FhirMapper;
import org.miracum.etl.fhirtoomop.mapper.PractitionerRoleMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

import static org.miracum.etl.fhirtoomop.Constants.PROCESSING_RESOURCES_LOG;

/**
 * The ConditionProcessor class represents the processing of FHIR PractitionerRole resources including the
 * mapping between FHIR and OMOP CDM.
 */
@Slf4j
public class PractitionerRoleProcessor extends ResourceProcessor<PractitionerRole> {

    /**
     * Constructor for objects of the class PractitionerRoleProcessor.
     *
     * @param mapper     mapper which maps FHIR resources to OMOP CDM
     * @param fhirParser parser which converts between the HAPI FHIR model/structure objects and their
     *                   respective String wire format (JSON)
     */
    public PractitionerRoleProcessor(PractitionerRoleMapper mapper, IParser fhirParser) {
        super(mapper, fhirParser);
    }

    @Override
    public OmopModelWrapper process(FhirPsqlResource fhirPsqlResource) {
        var r = fhirParser.parseResource(PractitionerRole.class, fhirPsqlResource.getData());
        log.debug(PROCESSING_RESOURCES_LOG, r.getResourceType(), r.getId());
        return mapper.map(
                r,
                fhirPsqlResource.getIsDeleted() == null ? Boolean.FALSE: fhirPsqlResource.getIsDeleted()
        );
    }
}
