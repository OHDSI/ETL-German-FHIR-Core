package org.miracum.etl.fhirtoomop.mapper;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.fhirpath.IFhirPath;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.VisitDetail;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IdMappingsTest extends MapperTestBase {
  private static final Logger log = LoggerFactory.getLogger(IdMappingsTest.class);

  private final Reference subject = new Reference("Patient/123");
  private PatientMapper patientMapper;
  private ConditionMapper conditionMapper;
  private IFhirPath fhirPath = new FhirPathR4(FhirContext.forR4());

  @BeforeEach
  void setup() {
    List<VisitDetail> visitDs = new ArrayList<>();
    visitDs.add(createVisitDetail(1L, 123L));
    List<Concept> concepts = new ArrayList<>();
    concepts.add(
        Concept.builder()
            .conceptCode("I12.3")
            .conceptId(1)
            .validStartDate(LocalDate.MIN)
            .validEndDate(LocalDate.MAX)
            .domainId("Condition")
            .build());

    patientMapper = new PatientMapper(fhirPath, true, dbMappings);
    conditionMapper = new ConditionMapper(true, dbMappings);
  }

  //  @Test
  //  void map_conditionWithReferenceToPatientWithBothIdentifierAndId_shouldResolveToSamePerson() {
  //    try {
  //      var mrConcept =
  //          new CodeableConcept()
  //              .setCoding(
  //                  List.of(new
  // Coding().setSystem(fhirSystems.getIdentifierType()).setCode("MR")));
  //
  //      // use different values for both the Patient.id and the Patient.identifier
  //      // both should be mapped to the same OMOP Person ID
  //      var identifier = new Identifier().setType(mrConcept).setValue("psn.123");
  //      var patient = new Patient().addIdentifier(identifier);
  //      patient.setId(new IdType(ResourceType.Patient.name(), "123"));
  //
  //      // setup the condition resource. This is mostly boilerplate.
  //      var icd =
  //          new CodeableConcept()
  //              .setCoding(
  //                  List.of(new Coding().setSystem(fhirSystems.getIcd10gm()).setCode("I12.3")));
  //      var condition =
  //          new Condition()
  //              .setSubject(subject)
  //              .setCode(icd)
  //              .setRecordedDate(Date.from(Instant.now()));
  //
  //      // First, map the Patient resource. This should result in two keys in the id mapping
  // table,
  //      // one for the identifier value and one for the Patient.id
  //      var personMapped = patientMapper.map(patient, isDeleted);
  //
  //      // Finally, map the Condition resource. It references the Patient using a reference.
  //      // The id mapping should support both mapping via a "full" reference as well as logical
  //      // reference.
  //
  //      var conditionMapped = conditionMapper.map(condition, isDeleted);
  //
  //      var personId = personMapped.getPerson().getPersonId();
  //      var conditionPersonId = conditionMapped.getConditionOccurrence().get(0).getPersonId();
  //
  //      assertThat(conditionPersonId).isEqualTo(personId);
  //    } catch (NullPointerException e) {
  //      log.warn(e.getMessage());
  //    }
  //  }
}
