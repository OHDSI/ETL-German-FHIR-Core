package org.miracum.etl.fhirtoomop.mapper;

import org.hl7.fhir.r4.model.CodeableConcept;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ConditionMapperTest extends MapperTestBase {
  private static final Logger log = LoggerFactory.getLogger(ConditionMapperTest.class);

  private ConditionMapper sut;
  private CodeableConcept icd;

  //  @BeforeEach
  //  void setUp() {
  //
  //    icd = new CodeableConcept();
  //    icd.addCoding().setSystem(fhirSystems.getIcd10gm()).setCode("I12.3");
  //
  //    sut = new ConditionMapper(fhirSystems, referenceUtils, true, dbMappings);
  //  }

  //    @Test
  //    void map_withConditionWithSubjectReferenceIdentifier_shouldUseIdentifierToLookupPersonId() {
  //      var identifierWithType = new Identifier().setType(mrConcept).setValue("123");
  //      var subjectReference = new Reference().setIdentifier(identifierWithType);
  //
  //      var condition =
  //          new Condition()
  //              .setSubject(subjectReference)
  //              .setCode(icd)
  //              .setRecordedDate(Date.from(Instant.now()));
  //      condition.setId("1");
  //
  //      var mapped = sut.map(condition);
  //
  //      assertThat(mapped.getConditionOccurrence()).hasSize(1);
  //      var conditionOccurrence = mapped.getConditionOccurrence().get(0);
  //      assertThat(conditionOccurrence.getPersonId()).isEqualTo(1L);
  //    }
  //
  //    @Test
  //    void map_withoutCode_shouldNotCreateConditionOccurrence() {
  //      var identifierWithType = new Identifier().setType(mrConcept).setValue("123");
  //      var subjectReference = new Reference().setIdentifier(identifierWithType);
  //
  //      var conditionWithNoCode =
  //          new
  // Condition().setSubject(subjectReference).setRecordedDate(Date.from(Instant.now()));
  //      conditionWithNoCode.setId("1");
  //
  //      var mapped = sut.map(conditionWithNoCode);
  //      assertThat(mapped).isNull();
  //    }
  //
  //    @Test
  //    void map_withoutIcdString__shouldNotCreateConditionOccurrence() {
  //      var identifierWithType = new Identifier().setType(mrConcept).setValue("123");
  //      var subjectReference = new Reference().setIdentifier(identifierWithType);
  //      icd.getCoding().get(0).setCode("");
  //      var conditionWithEmptyIcdString =
  //          new Condition()
  //              .setSubject(subjectReference)
  //              .setCode(icd)
  //              .setRecordedDate(Date.from(Instant.now()));
  //      conditionWithEmptyIcdString.setId("1");
  //      var maper = sut.map(conditionWithEmptyIcdString);
  //      assertThat(maper).isNull();
  //    }
  //
  //    @Test
  //    void map_withEitherSubjectReference_shouldCreateConditionOccurrence() {
  //      var identifierWithType = new Identifier().setType(mrConcept).setValue("123");
  //      var subjectIdentifierReference = new Reference().setIdentifier(identifierWithType);
  //      var subjectIdReference = new Reference("Patient/456");
  //
  //      var conditionWithId = new Condition();
  //      conditionWithId
  //          .setSubject(subjectIdReference)
  //          .setCode(icd)
  //          .setRecordedDate(Date.from(Instant.now()));
  //      conditionWithId.setId("1");
  //
  //      var conditionWithIdentfier = new Condition();
  //      conditionWithIdentfier
  //          .setSubject(subjectIdentifierReference)
  //          .setCode(icd)
  //          .setRecordedDate(Date.from(Instant.now()));
  //      conditionWithIdentfier.setId("2");
  //
  //      var map1 = sut.map(conditionWithId);
  //      var map2 = sut.map(conditionWithIdentfier);
  //      assertThat(map1.getConditionOccurrence().size())
  //          .isEqualTo(map2.getConditionOccurrence().size());
  //      var conditionOcc1 = map1.getConditionOccurrence().get(0);
  //      var conditionOcc2 = map2.getConditionOccurrence().get(0);
  //      assertThat(conditionOcc1.getPersonId()).isEqualTo(conditionOcc2.getPersonId());
  //    }
  //
  //    @Test
  //    void map_withoutSubjectReference_shouldNotCreateConditionOccurrence() {
  //
  //      var condition = new Condition().setCode(icd).setRecordedDate(Date.from(Instant.now()));
  //      condition.setId("1");
  //
  //      var mapped = sut.map(condition);
  //
  //      assertThat(mapped).isNull();
  //    }
}
