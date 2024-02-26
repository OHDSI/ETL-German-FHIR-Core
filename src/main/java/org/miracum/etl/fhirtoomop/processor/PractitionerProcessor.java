package org.miracum.etl.fhirtoomop.processor;

import ca.uhn.fhir.parser.IParser;
import lombok.extern.slf4j.Slf4j;
import org.hl7.fhir.r4.model.Practitioner;
import org.miracum.etl.fhirtoomop.mapper.PractitionerMapper;
import org.miracum.etl.fhirtoomop.model.FhirPsqlResource;
import org.miracum.etl.fhirtoomop.model.OmopModelWrapper;

import static org.miracum.etl.fhirtoomop.Constants.PROCESSING_RESOURCES_LOG;



@Slf4j
public class PractitionerProcessor extends ResourceProcessor<Practitioner> {

    /**
     * Constructor for objects of the class PractitionerProcessor.
     *
     * @param mapper mapper which maps FHIR Practitioner resources to OMOP CDM
     * @param parser parser which converts between the HAPI FHIR model/structure objects and their
     *               respective String wire format (JSON)
     */
    public PractitionerProcessor(PractitionerMapper mapper, IParser parser) {
        super(mapper, parser);
    }

    /**
     * Processes FHIR Practitioner resources and maps them to OMOP CDM.
     *
     * @param fhirPsqlResource FHIR resource and its metadata from FHIR Gateway
     * @return wrapper with objects to be written to OMOP CDM
     */
    @Override
    public OmopModelWrapper process(FhirPsqlResource fhirPsqlResource) {

        var r = fhirParser.parseResource(Practitioner.class, fhirPsqlResource.getData());
        log.debug(PROCESSING_RESOURCES_LOG, r.getResourceType(), r.getId());
        return mapper.map(
                r,
                fhirPsqlResource.getIsDeleted() == null ? Boolean.FALSE : fhirPsqlResource.getIsDeleted());
    }
}