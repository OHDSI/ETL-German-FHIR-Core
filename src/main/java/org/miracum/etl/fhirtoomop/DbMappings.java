package org.miracum.etl.fhirtoomop;

import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.miracum.etl.fhirtoomop.model.AtcStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.IcdSnomedDomainLookup;
import org.miracum.etl.fhirtoomop.model.LoincStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.MedicationIdMap;
import org.miracum.etl.fhirtoomop.model.OpsStandardDomainLookup;
import org.miracum.etl.fhirtoomop.model.OrphaSnomedMapping;
import org.miracum.etl.fhirtoomop.model.PostProcessMap;
import org.miracum.etl.fhirtoomop.model.SnomedRaceStandardLookup;
import org.miracum.etl.fhirtoomop.model.SnomedVaccineStandardLookup;
import org.miracum.etl.fhirtoomop.model.omop.CareSite;
import org.miracum.etl.fhirtoomop.model.omop.ConditionOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.Measurement;
import org.miracum.etl.fhirtoomop.model.omop.OmopObservation;
import org.miracum.etl.fhirtoomop.model.omop.Person;
import org.miracum.etl.fhirtoomop.model.omop.ProcedureOccurrence;
import org.miracum.etl.fhirtoomop.model.omop.SourceToConceptMap;
import org.miracum.etl.fhirtoomop.model.omop.VisitDetail;
import org.miracum.etl.fhirtoomop.model.omop.VisitOccurrence;

/**
 * The class DbMappings contains collections for the intermediate storage of data in RAM, which were
 * loaded from OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class DbMappings {
  private Map<Long, Person> findPersonIdByReference;
  private Map<String, Long> findPersonIdByLogicalId;
  private Map<String, Long> findPersonIdByIdentifier;

  private Map<Long, VisitOccurrence> findVisitOccIdByReference;
  private Map<String, Long> findVisitOccIdByLogicalId;
  private Map<String, Long> findVisitOccIdByIdentifier;

  private Map<String, Long> findLocationIdBySourceValue;
  private Map<String, List<SourceToConceptMap>> findHardCodeConcept;
  private Map<String, CareSite> findCareSiteId;
  private Map<String, PostProcessMap> findRanking;

  private OmopConceptMapWrapper omopConceptMapWrapper = new OmopConceptMapWrapper();
  //  private Map<String, List<Concept>> findValidLoincConceptId;
  //  private Map<String, List<Concept>> findValidUcumConceptId;
  //  private Map<String, List<Concept>> findValidAtcConceptId;
  //  private Map<String, List<Concept>> findValidOpsConceptId;
  //  private Map<String, List<Concept>> findValidSnomedConceptId;

  private Map<String, List<IcdSnomedDomainLookup>> findIcdSnomedMapping;
  private Map<String, List<SnomedVaccineStandardLookup>> findSnomedVaccineMapping;
  private Map<String, SnomedRaceStandardLookup> findSnomedRaceStandardMapping;
  private Map<String, List<OrphaSnomedMapping>> findOrphaSnomedMapping;
  private Map<String, List<OpsStandardDomainLookup>> findOpsStandardMapping;
  private Map<String, List<AtcStandardDomainLookup>> findAtcStandardMapping;
  private Map<String, List<LoincStandardDomainLookup>> findLoincStandardMapping;

  private Map<Long, List<VisitDetail>> findAllVisitDetails;
  private Map<Long, List<ConditionOccurrence>> findConditionOccurrence;
  private Map<Long, List<ProcedureOccurrence>> findProcedureOccurrence;
  private Map<Long, List<OmopObservation>> findOmopObservation;
  private Map<Long, List<Measurement>> findMeasurement;
  private Map<String, List<MedicationIdMap>> findMedication;
}
