package org.miracum.etl.fhirtoomop.mapper;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.fhirpath.IFhirPath;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.hl7.fhir.r4.model.Reference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class EncounterMainMapperTest extends MapperTestBase {
  private final Reference subject = new Reference("Patient/456");
  private IFhirPath fhirPath = new FhirPathR4(FhirContext.forR4());

  @InjectMocks
  private EncounterInstitutionContactMapper encounterMainMapper =
      new EncounterInstitutionContactMapper(referenceUtils, true, dbMappings);

  @Mock private ResourceOmopReferenceUtils omopReferenceUtils;

  @BeforeEach
  public void setUp() {

    //    sut =
    //        new EncounterInstitutionContactMapper(
    //            idMappings, fhirSystems, fhirPath, referenceUtils, true, dbMappings,
    // repositories);
  }

  //  @Test
  //  void map_withIdentifierWithEncounterIdType_shouldUseIdentifierValueAsSourceValue() {
  //
  //    var expectedIdentifier = "123";
  //    var identifierWithType = new Identifier().setType(vnConcept).setValue(expectedIdentifier);
  //    var encounter = new Encounter().addIdentifier(identifierWithType).setSubject(subject);
  //    encounter.setId("1");
  //
  //    var mapped = sut.map(encounter);
  //
  //    assertThat(mapped.getVisitOccurrence().getVisitSourceValue()).isEqualTo(expectedIdentifier);
  //  }

  //  @Test
  //  void map_withoutIdentifiers_shouldSetEncounterSourceValueNull() {
  //    var identifierWithType = new Identifier().setType(vnConcept).setValue("123");
  //
  //    Encounter encounter = new Encounter().setSubject(subject);
  //    //    encounter.addIdentifier(identifierWithType);
  //    encounter.setId("1");
  //    ResourceOmopReferenceUtils omopRef =
  //        new ResourceOmopReferenceUtils(true, dbMappings, repositories, null, null);
  //    var personId = omopRef.getPersonId(null, "456", encounter.getId());
  //    assertThat(personId).isEqualTo(1L);
  //    var mapped = sut.map(encounter);
  //
  //    assertThat(mapped.getVisitOccurrence().getVisitSourceValue()).isNull();
  //  }

  //  @Test
  //  void map_withoutAnyId_returnsNull() {
  //
  //    var encounter = new Encounter().setSubject(subject);
  //
  //    assertThat(encounterMainMapper.map(encounter, isDeleted)).isNull();
  //  }

  //  @Test
  //  void map_withIdLongerThan50Characters_shouldTruncate() {
  //
  //    // creates a 64 characters long string
  //    @SuppressWarnings("UnstableApiUsage")
  //    var sha256hex = Hashing.sha256().hashString("123", StandardCharsets.UTF_8).toString();
  //    var identifierWithType = new Identifier().setType(vnConcept).setValue(sha256hex);
  //    var encounter = new Encounter().addIdentifier(identifierWithType).setSubject(subject);
  //
  //    var mapped = sut.map(encounter);
  //
  //    assertThat(sha256hex).hasSizeGreaterThan(50);
  //    assertThat(mapped.getVisitOccurrence().getVisitSourceValue()).hasSizeLessThanOrEqualTo(50);
  //  }
}
