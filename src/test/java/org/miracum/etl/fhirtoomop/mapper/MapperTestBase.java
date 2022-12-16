package org.miracum.etl.fhirtoomop.mapper;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.PostConstruct;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.miracum.etl.fhirtoomop.DbMappings;
import org.miracum.etl.fhirtoomop.IIdMappings;
import org.miracum.etl.fhirtoomop.InMemoryIncrementalIdMappings;
import org.miracum.etl.fhirtoomop.config.FhirConfig;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.miracum.etl.fhirtoomop.model.IcdSnomedDomainLookup;
import org.miracum.etl.fhirtoomop.model.omop.Concept;
import org.miracum.etl.fhirtoomop.model.omop.Concept.Domain;
import org.miracum.etl.fhirtoomop.model.omop.Concept.Vocabulary;
import org.miracum.etl.fhirtoomop.model.omop.Person;
import org.miracum.etl.fhirtoomop.model.omop.VisitDetail;
import org.miracum.etl.fhirtoomop.model.omop.VisitOccurrence;
import org.miracum.etl.fhirtoomop.repository.OmopRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = {FhirConfig.class})
@ActiveProfiles("test")
@EnableConfigurationProperties(value = {FhirSystems.class})
public class MapperTestBase {

  @Autowired FhirSystems fhirSystems;

  @Autowired ResourceFhirReferenceUtils referenceUtils;
  protected CodeableConcept mrConcept;
  protected CodeableConcept vnConcept;
  protected IIdMappings idMappings;
  protected JdbcTemplate jdbcTemplate;
  protected OmopRepository repositories;
  protected DbMappings dbMappings;
  protected boolean isDeleted;

  @PostConstruct
  public void setupConcepts() {

    mrConcept =
        new CodeableConcept()
            .addCoding(new Coding().setSystem(fhirSystems.getIdentifierType()).setCode("MR"));
    vnConcept =
        new CodeableConcept()
            .addCoding(new Coding().setSystem(fhirSystems.getIdentifierType()).setCode("VN"));

    idMappings = new InMemoryIncrementalIdMappings();
    dbMappings = new DbMappings();
    repositories = new OmopRepository();

    dbMappings.setFindPersonIdByReference(createSingletonPersonMap(1L, createPerson()));
    dbMappings.setFindVisitOccIdByReference(
        createSingletonEncounterMap(2L, createVisitOccurrence()));

    dbMappings.setFindIcdSnomedMapping(createSingletonMap("I12.3", createIcdSnomedLookup()));
    dbMappings
        .getOmopConceptMapWrapper()
        .setFindValidLoincConcept(
            createSingletonMap("95826-4", 1, Vocabulary.LOINC, Domain.OBSERVATION));
    dbMappings
        .getOmopConceptMapWrapper()
        .setFindValidLoincConcept(
            createSingletonMap("2160-0", 2, Vocabulary.LOINC, Domain.MEASUREMENT));
  }

  protected Person createPerson() {
    return Person.builder().personId(1L).fhirLogicalId("456").fhirIdentifier("123").build();
  }

  protected VisitOccurrence createVisitOccurrence() {
    return VisitOccurrence.builder()
        .visitOccurrenceId(2L)
        .fhirLogicalId("456")
        .fhirIdentifier("123")
        .build();
  }

  protected Map<String, List<Concept>> createSingletonMap(
      String conceptCode, int conceptId, Vocabulary vocabulary, Domain domain) {
    List<Concept> list = new ArrayList<>();
    list.add(createConcept(conceptCode, conceptId, vocabulary, domain));
    return Collections.singletonMap(conceptCode, list);
  }

  protected Map<Long, Person> createSingletonPersonMap(Long personId, Person person) {
    return Collections.singletonMap(personId, person);
  }

  protected Map<Long, VisitOccurrence> createSingletonEncounterMap(
      Long visitOccId, VisitOccurrence visitOcc) {
    return Collections.singletonMap(visitOccId, visitOcc);
  }

  protected Map<String, List<IcdSnomedDomainLookup>> createSingletonMap(
      String icdCode, IcdSnomedDomainLookup icdSnomed) {
    List<IcdSnomedDomainLookup> list = new ArrayList<>();
    list.add(icdSnomed);
    return Collections.singletonMap(icdCode, list);
  }

  protected static IcdSnomedDomainLookup createIcdSnomedLookup() {
    return IcdSnomedDomainLookup.builder()
        .icdGmCode("I12.3")
        .icdGmConceptId(123)
        .snomedConceptId(456)
        .icdGmValidStartDate(LocalDate.MIN)
        .icdGmValidEndDate(LocalDate.MAX)
        .snomedDomainId("Condition")
        .build();
  }

  protected static Concept createConcept(
      String conceptCode, int conceptId, Vocabulary vocabulary, Domain domain) {
    return Concept.builder()
        .conceptId(conceptId)
        .conceptCode(conceptCode)
        .validStartDate(LocalDate.MIN)
        .validEndDate(LocalDate.MAX)
        .vocabularyId(vocabulary.getLabel())
        .domainId(domain.getLabel())
        .build();
  }

  protected static VisitDetail createVisitDetail(Long visit_detail_id, Long person_id) {
    return VisitDetail.builder()
        .visitDetailId(visit_detail_id)
        .personId(person_id)
        .visitDetailStartDate(LocalDate.MIN)
        .visitDetailEndDate(LocalDate.MAX)
        .visitOccurrenceId(1L)
        .visitDetailTypeConceptId(32817)
        .build();
  }
}
