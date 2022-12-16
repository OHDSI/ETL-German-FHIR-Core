package org.miracum.etl.fhirtoomop.repository.service;

import java.util.List;
import org.miracum.etl.fhirtoomop.repository.ConditionOccRepository;
import org.miracum.etl.fhirtoomop.repository.DrugExposureRepository;
import org.miracum.etl.fhirtoomop.repository.MeasurementRepository;
import org.miracum.etl.fhirtoomop.repository.ObservationRepository;
import org.miracum.etl.fhirtoomop.repository.VisitDetailRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The EncounterDepartmentCaseMapperServiceImpl class contains the specific implementation to access
 * data trough OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("EncounterDepartmentCaseMapperServiceImpl")
public class EncounterDepartmentCaseMapperServiceImpl {
  @Autowired VisitDetailRepository visitDetailRepository;
  @Autowired ObservationRepository observationRepository;
  @Autowired MeasurementRepository measurementRepository;
  @Autowired ConditionOccRepository conditionOccRepository;
  @Autowired DrugExposureRepository drugExposureRepository;

  /**
   * Sets all admitting_source_value, discharge_to_source_value, preceding_visit_detail_id and
   * visit_detail_parent_id values in visit_detail table in OMOP CDM to NULL, based on the
   * visit_occurrence_id.
   *
   * @param existingVisitDetailIds visit_detail_id of existing visit_details in OMOP CDM
   * @param visitOccId visit_occurrence_id of existing visit_details in OMOP CDM
   */
  public void resetVisitDetailIds(List<Long> existingVisitDetailIds, Long visitOccId) {
    measurementRepository.updateVisitDetailIds(existingVisitDetailIds);
    observationRepository.updateVisitDetailIds(existingVisitDetailIds);
    drugExposureRepository.updateVisitDetailIds(existingVisitDetailIds);
    conditionOccRepository.updateVisitDetailIds(existingVisitDetailIds);
    visitDetailRepository.updateReferencedVisitDetailIds(visitOccId);
    visitDetailRepository.flush();
  }

  /**
   * Delete FHIR Encounter resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Encounter resource
   */
  public void deleteExistingDepartmentcaseByLogicalId(String fhirLogicalId) {
    visitDetailRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR Encounter resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Encounter resource
   */
  public void deleteExistingDepartmentcaseByIdentifier(String fhirIdentifier) {
    visitDetailRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
