package org.miracum.etl.fhirtoomop.repository;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The OmopRepository class represents a summary of OMOP CDM repositories that are used during the
 * ETL process.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Getter
@Component("repositories")
public class OmopRepository {
  @Autowired private ConceptRepository conceptRepository;
  @Autowired private VisitDetailRepository visitDetailRepository;
  @Autowired private CareSiteRepository careSiteRepository;
  @Autowired private MedicationIdRepository medicationIdRepository;
  @Autowired private ConditionOccRepository conditionOccRepository;
  @Autowired private MeasurementRepository measurementRepository;
  @Autowired private ObservationRepository observationRepository;
  @Autowired private ProviderRepository providerRepository;
  @Autowired private ProcedureOccRepository procedureOccRepository;
  @Autowired private SourceToConceptRepository sourceToConceptRepository;
  @Autowired private IcdSnomedRepository icdSnomedRepository;
  @Autowired private PersonRepository personRepository;
  @Autowired private LocationRepository locationRepository;
  @Autowired private PostProcessMapRepository postProcessMapRepository;
  @Autowired private VisitOccRepository visitOccRepository;
  @Autowired private FactRelationshipRepository factRelationshipRepository;
  @Autowired private DrugExposureRepository drugExposureRepository;
  @Autowired private DeviceExposureRepository deviceExposureRepository;
  @Autowired private SpecimenRepository specimenRepository;
  @Autowired private SnomedVaccineRepository snomedVaccineRepository;
  @Autowired private SnomedRaceRepository snomedRaceRepository;
  @Autowired private OrphaSnomedMappingRepository orphaSnomedMappingRepository;
  @Autowired private OpsStandardRepository opsStandardRepository;
  @Autowired private AtcStandardRepository atcStandardRepository;
  @Autowired private StandardRepository standardRepository;
}
