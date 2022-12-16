package org.miracum.etl.fhirtoomop.mapper;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProcedureMapperTest extends MapperTestBase {
  private static final Logger log = LoggerFactory.getLogger(ProcedureMapperTest.class);
  private ProcedureMapper sut;

  private static final Reference subjectReference = new Reference("Patient/123");

  @BeforeEach
  void setUp() {
    List<Concept> concepts = new ArrayList<>();
    concepts.add(
        Concept.builder()
            .conceptCode("1-100")
            .conceptId(1)
            .validStartDate(LocalDate.MIN)
            .validEndDate(LocalDate.MAX)
            .domainId("Procedure")
            .build());

    sut = new ProcedureMapper(true, dbMappings);

    idMappings.getPersons().put("123", 123L);
  }

  //  @Test
  //  void map_withSomeOpsCode_shouldMapToExpectedConceptAndTimestamp() {
  //    try {
  //      var ops =
  //          new CodeableConcept()
  //              .setCoding(List.of(new
  // Coding().setSystem(fhirSystems.getOps()).setCode("1-100")));
  //
  //      var expectedProcedureTime = LocalDateTime.now();
  //      var procedure =
  //          new Procedure()
  //              .setSubject(subjectReference)
  //              .setCode(ops)
  //              .setPerformed(new DateTimeType(Timestamp.valueOf(expectedProcedureTime)));
  //
  //      var mapped = sut.map(procedure, isDeleted);
  //
  //      assertThat(mapped.getProcedureOccurrence()).hasSize(1);
  //      var procedureOccurrence = mapped.getProcedureOccurrence().get(0);
  //
  //      assertThat(procedureOccurrence.getProcedureConceptId()).isEqualTo(1);
  //      assertThat(procedureOccurrence.getProcedureDatetime())
  //          .isCloseTo(expectedProcedureTime, within(1, ChronoUnit.SECONDS));
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  void map_withoutEncounterReference_shouldCreateProcedureOccurrenceWithoutVisit() {
  //    try {
  //      var ops =
  //          new CodeableConcept()
  //              .setCoding(List.of(new
  // Coding().setSystem(fhirSystems.getOps()).setCode("1-100")));
  //
  //      var expectedProcedureTime = LocalDateTime.now();
  //      var procedure =
  //          new Procedure()
  //              .setSubject(subjectReference)
  //              .setCode(ops)
  //              .setPerformed(new DateTimeType(Timestamp.valueOf(expectedProcedureTime)));
  //
  //      var mapped = sut.map(procedure, isDeleted);
  //      var obs = mapped.getProcedureOccurrence().get(0);
  //
  //      assertThat(obs.getVisitOccurrenceId()).isNull();
  //      assertThat(obs.getVisitDetailId()).isNull();
  //      assertThat(obs.getPersonId()).isEqualTo(123);
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
}
