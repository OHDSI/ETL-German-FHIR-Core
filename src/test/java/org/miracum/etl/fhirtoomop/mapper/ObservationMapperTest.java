package org.miracum.etl.fhirtoomop.mapper;

import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ObservationMapperTest extends MapperTestBase {
  private static final Logger log = LoggerFactory.getLogger(ObservationMapperTest.class);
  private ObservationMapper sut;

  private static final Reference subjectReference = new Reference("Patient/123");

  private Observation baseObservation;

  //  @BeforeEach
  //  void setUp() {
  //    //    List<Concept> concepts = new ArrayList<>();
  //    //    concepts.add(createConcept("95826-4", 1, Vocabulary.LOINC, Domain.OBSERVATION));
  //    //    concepts.add(createConcept("2160-0", 2, Vocabulary.LOINC, Domain.MEASUREMENT));
  //    //    concepts.add(createConcept("15074-8", 3, Vocabulary.LOINC, Domain.MEASUREMENT));
  //    //    concepts.add(createConcept("72089-6", 42869844, Vocabulary.LOINC,
  // Domain.OBSERVATION));
  //
  //    sut = new ObservationMapper(fhirSystems, referenceUtils, true, dbMappings);
  //
  //    var loinc = new CodeableConcept();
  //    loinc.addCoding().setSystem(fhirSystems.getLoinc()).setCode("2160-0");
  //    var value =
  //        new Quantity()
  //            .setValue(6.3)
  //            .setUnit("mmol/l")
  //            .setSystem(fhirSystems.getUcum())
  //            .setCode("mmol/l");
  //    baseObservation =
  //        new Observation()
  //            .setSubject(subjectReference)
  //            .setCode(loinc)
  //            .setValue(value)
  //            .setEffective(new DateTimeType().setValue(Date.from(Instant.now())));
  //    baseObservation.setId("1");
  //  }
  //
  //  @Test
  //  void map_withUnmappedLoinc_shouldNotCreateObservation() {
  //    try {
  //      var loinc = new CodeableConcept();
  //      loinc.addCoding().setSystem(fhirSystems.getLoinc()).setCode("not-mapped");
  //      var observation = baseObservation.setCode(loinc);
  //
  //      var mapped = sut.map(observation, isDeleted);
  //
  //      assertThat(mapped).isNull();
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  void map_withMappedLoinc_shouldCreateObservation() {
  //    try {
  //      var loinc = new CodeableConcept();
  //      loinc.addCoding().setSystem(fhirSystems.getLoinc()).setCode("95826-4");
  //      var observation = baseObservation.setCode(loinc);
  //
  //      var mapped = sut.map(observation, isDeleted);
  //
  //      assertThat(mapped.getObservation()).hasSize(1);
  //
  //      var obsOccurrence = mapped.getObservation().get(0);
  //      assertThat(obsOccurrence.getObservationConceptId()).isEqualTo(1);
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  void map_loincWithFloatingPointValue_shouldRetainFloatingValue() {
  //    try {
  //      var loinc = new CodeableConcept();
  //      loinc.addCoding().setSystem(fhirSystems.getLoinc()).setCode("2160-0");
  //      var observation = baseObservation.setValue(new Quantity().setValue(1.23));
  //
  //      var mapped = sut.map(observation, isDeleted);
  //      var obs = mapped.getMeasurement().get(0);
  //
  //      assertThat(obs.getValueAsNumber()).isEqualTo(BigDecimal.valueOf(1.23));
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
  //
  //  @ParameterizedTest
  //  @CsvSource({"3.1,6.2,3.1,6.2", "3.1,,3.1,", ",6.2,,6.2", ",,,,"})
  //  void map_withGivenCombinationOfReferenceRanges_shouldApplyThemCorrectly(
  //      BigDecimal givenLow, BigDecimal givenHigh, BigDecimal expectedLow, BigDecimal
  // expectedHigh) {
  //    try {
  //      var loinc = new CodeableConcept();
  //      loinc.addCoding().setSystem(fhirSystems.getLoinc()).setCode("15074-8");
  //      var value =
  //          new Quantity()
  //              .setValue(6.3)
  //              .setUnit("mmol/l")
  //              .setSystem(fhirSystems.getUcum())
  //              .setCode("mmol/l");
  //
  //      var refRange = new ObservationReferenceRangeComponent();
  //
  //      if (givenLow != null) {
  //        var low =
  //            new Quantity()
  //                .setValue(givenLow)
  //                .setSystem(value.getSystem())
  //                .setUnit(value.getUnit())
  //                .setCode(value.getCode());
  //        refRange.setLow(low);
  //      }
  //
  //      if (givenHigh != null) {
  //        var high =
  //            new Quantity()
  //                .setValue(givenHigh)
  //                .setSystem(value.getSystem())
  //                .setUnit(value.getUnit())
  //                .setCode(value.getCode());
  //        refRange.setHigh(high);
  //      }
  //
  //      var observation = baseObservation.setReferenceRange(List.of(refRange));
  //
  //      var mapped = sut.map(observation, isDeleted);
  //      var obs = mapped.getMeasurement().get(0);
  //
  //      assertThat(obs.getValueAsNumber()).isEqualTo(value.getValue());
  //      assertThat(obs.getRangeLow()).isEqualTo(expectedLow);
  //      assertThat(obs.getRangeHigh()).isEqualTo(expectedHigh);
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  void map_totalNihssScore_shouldBeMappedToObservation() {
  //    try {
  //      var loinc = new CodeableConcept();
  //      loinc.addCoding().setSystem(fhirSystems.getLoinc()).setCode("72089-6");
  //      var observation =
  //          baseObservation
  //              .setCode(loinc)
  //              .setValue(
  //                  new
  // Quantity().setValue(15).setCode("{score}").setSystem(fhirSystems.getUcum()));
  //
  //      var mapped = sut.map(observation, isDeleted);
  //      var obs = mapped.getObservation().get(0);
  //
  //      assertThat(obs.getValueAsNumber()).isEqualTo(BigDecimal.valueOf(15));
  //      assertThat(obs.getObservationConceptId()).isEqualTo(42869844);
  //      assertThat(obs.getUnitSourceValue()).isEqualTo("{score}");
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
  //
  //  @Test
  //  void map_withoutEncounterReference_shouldCreateObservationOccurrenceWithoutVisit() {
  //    try {
  //      var observation = baseObservation.setEncounter(null);
  //
  //      var mapped = sut.map(observation, isDeleted);
  //      var obs = mapped.getMeasurement().get(0);
  //
  //      assertThat(obs.getVisitOccurrenceId()).isNull();
  //      assertThat(obs.getVisitDetailId()).isNull();
  //      assertThat(obs.getPersonId()).isEqualTo(123);
  //    } catch (NullPointerException e) {
  //      log.error(e.getMessage());
  //    }
  //  }
}
