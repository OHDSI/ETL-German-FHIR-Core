package org.miracum.etl.fhirtoomop.mapper;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.fhirpath.IFhirPath;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.junit.runner.RunWith;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceOmopReferenceUtils;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
class PatientMapperTest extends MapperTestBase {

  @Mock private ResourceOmopReferenceUtils omopReferenceUtils;
  @Mock private ResourceFhirReferenceUtils fhirReferenceUtils;

  private IFhirPath fhirPath = new FhirPathR4(FhirContext.forR4());
  private DbMappings dbMappings = new DbMappings();

  @InjectMocks private PatientMapper sut = new PatientMapper(fhirPath, true, dbMappings);

  //  @Test
  //  void map_withIdentifierWithPatientIdType_shouldUseIdentifierValueAsPersonSourceValue() {
  //    try {
  //      var expectedIdentifier = "123";
  //      var mrConcept = new CodeableConcept();
  //      mrConcept.addCoding().setSystem(fhirSystems.getIdentifierType()).setCode("MR");
  //      var identifierWithType = new Identifier().setType(mrConcept).setValue(expectedIdentifier);
  //      var patient = new Patient().addIdentifier(identifierWithType);
  //
  //      var mapped = sut.map(patient, isDeleted);
  //
  //      assertThat(mapped.getPerson().getPersonSourceValue()).isEqualTo(expectedIdentifier);
  //    } catch (Exception ignore) {
  //
  //    }
  //  }

  //  @Test
  //  void map_withNoIdentifiers_shouldSetPersonSourceValueNull() {
  //
  //    var expectedIdentifier = "Patient/123";
  //    var patient = new Patient();
  //    patient.setId(expectedIdentifier);
  //    when(fhirReferenceUtils.extractId(patient)).thenReturn(expectedIdentifier);
  //    var mapped = sut.map(patient, isDeleted);
  //
  //    assertThat(mapped.getPerson().getPersonSourceValue()).isNullOrEmpty();
  //  }

  //  @Test
  //  void map_withoutAnyPatientId_shouldReturnNull() {
  //
  //    var patient = new Patient();
  //
  //    var mapped = sut.map(patient, isDeleted);
  //
  //    assertThat(mapped).isNull();
  //  }
  //
  //  @Test
  //  void map_withPatientIdLongerThan50Characters_shouldTruncate() {
  //    try {
  //      // creates a 64 characters long string
  //      @SuppressWarnings("UnstableApiUsage")
  //      var sha256hex = Hashing.sha256().hashString("123", StandardCharsets.UTF_8).toString();
  //
  //      var mrConcept = new CodeableConcept();
  //      mrConcept.addCoding().setSystem(fhirSystems.getIdentifierType()).setCode("MR");
  //      var identifierWithType = new Identifier().setType(mrConcept).setValue(sha256hex);
  //      var patient = new Patient().addIdentifier(identifierWithType);
  //
  //      var mapped = sut.map(patient, isDeleted);
  //
  //      assertThat(sha256hex).hasSizeGreaterThan(50);
  //      assertThat(mapped.getPerson().getPersonSourceValue()).hasSizeLessThanOrEqualTo(50);
  //    } catch (Exception ignore) {
  //
  //    }
  //  }
  //
  //  @Test
  //  void map_withBirthdateAbsentExtensionAndValue_shouldSetBirthdateToValue() {
  //
  //    var patient = new Patient();
  //    patient.setId("Patient/1");
  //    var absentBirthDate = new DateType();
  //    absentBirthDate.addExtension(fhirSystems.getDataAbsentReason(), new CodeType("unknown"));
  //    absentBirthDate.setValueAsString("1970-01-27");
  //    patient.setBirthDateElement(absentBirthDate);
  //    when(fhirReferenceUtils.extractId(patient)).thenReturn("1");
  //
  //    var mapped = sut.map(patient, isDeleted);
  //
  //    assertThat(mapped.getPerson().getYearOfBirth()).isEqualTo(1970);
  //    assertThat(mapped.getPerson().getDayOfBirth()).isEqualTo(27);
  //    assertThat(mapped.getPerson().getMonthOfBirth()).isEqualTo(1);
  //    assertThat(mapped.getPerson().getBirthDatetime()).isNull();
  //  }
  //
  //  @Test
  //  void map_withBirthYearAndMonth_shouldSetOnlyYearAndMonth() {
  //
  //    var patient = new Patient();
  //    patient.setId("Patient/1");
  //    var birthDate = new DateType();
  //    birthDate.setValueAsString("1971-12");
  //    patient.setBirthDateElement(birthDate);
  //    when(fhirReferenceUtils.extractId(patient)).thenReturn("1");
  //    var mapped = sut.map(patient, isDeleted);
  //
  //    assertThat(mapped.getPerson().getYearOfBirth()).isEqualTo(1971);
  //    assertThat(mapped.getPerson().getMonthOfBirth()).isEqualTo(12);
  //    assertThat(mapped.getPerson().getDayOfBirth()).isNull();
  //    assertThat(mapped.getPerson().getBirthDatetime()).isNull();
  //  }
  //
  //  @Test
  //  void map_withOnlyBirthYear_shouldSetBirthDayAndMonthToNull() {
  //
  //    var patient = new Patient();
  //    patient.setId("Patient/1");
  //    var year = new DateType();
  //    year.setValueAsString("2000");
  //    patient.setBirthDateElement(year);
  //    when(fhirReferenceUtils.extractId(patient)).thenReturn("1");
  //    var mapped = sut.map(patient, isDeleted);
  //
  //    var person = mapped.getPerson();
  //    assertThat(person.getYearOfBirth()).isEqualTo(2000);
  //    assertThat(person.getDayOfBirth()).isNull();
  //    assertThat(person.getMonthOfBirth()).isNull();
  //    assertThat(person.getBirthDatetime()).isNull();
  //  }

  //  @Test
  //  void map_withAddressAbsent_shouldUseUnknownLocation() {
  //
  //    var patient = new Patient();
  //    patient.setId("1");
  //    var absentAddress = new Address();
  //    absentAddress.addExtension(fhirSystems.getDataAbsentReason(), new CodeType("unknown"));
  //    patient.addAddress(absentAddress);
  //
  //    var mapped = sut.map(patient);
  //
  //    assertThat(mapped.getLocation().getLocationSourceValue()).isEmpty();
  //  }

  //  @Test
  //  void map_withAddress_shouldSetLocationSourceValueToTextualRepresentation() {
  //
  //    var patient = new Patient();
  //    patient.setId("1");
  //    patient
  //        .addAddress()
  //        .setPostalCode("12345")
  //        .setCity("City")
  //        .setDistrict("District")
  //        .setState("State")
  //        .setType(AddressType.PHYSICAL);
  //
  //    var mapped = sut.map(patient);
  //
  //    assertThat(mapped.getLocation().getLocationSourceValue())
  //        .isEqualTo("12345 City District State");
  //  }

  //  @Test
  //  void map_withOverlongAddress_shouldAbbreviateTooLongValues() {
  //
  //    var patient = new Patient();
  //    patient.setId("1");
  //    patient
  //        .addAddress()
  //        .setPostalCode("1234567890") // longer than MAX_LOCATION_ZIP_LENGTH=9
  //        .setCity("A very long City but less than 50 characters")
  //        .setDistrict("District")
  //        .setState("State")
  //        .setType(AddressType.PHYSICAL);
  //
  //    var mapped = sut.map(patient);
  //
  //    assertThat(mapped.getLocation().getZip())
  //        .isEqualTo(
  //            StringUtils.left(
  //                patient.getAddressFirstRep().getPostalCode(), MAX_LOCATION_ZIP_LENGTH));
  //    assertThat(mapped.getLocation().getState()).isEqualTo("St");
  //
  // assertThat(mapped.getLocation().getCity()).isEqualTo(patient.getAddressFirstRep().getCity());
  //  }
}
