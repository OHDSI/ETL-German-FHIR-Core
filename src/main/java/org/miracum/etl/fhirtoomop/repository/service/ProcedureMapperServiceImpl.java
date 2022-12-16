package org.miracum.etl.fhirtoomop.repository.service;

import org.miracum.etl.fhirtoomop.repository.ProcedureOccRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The ProcedureMapperServiceImpl class contains the specific implementation to access data trough
 * OMOP repositories.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Transactional("transactionManager")
@Service("ProcedureMapperServiceImpl")
public class ProcedureMapperServiceImpl {
  @Autowired ProcedureOccRepository procedureOccRepository;

  /**
   * Delete FHIR Procedure resources from OMOP CDM tables using fhir_logical_id
   *
   * @param fhirLogicalId logical id of the FHIR Procedure resource
   */
  public void deleteExistingProcedureOccByFhirLogicalId(String fhirLogicalId) {
    procedureOccRepository.deleteByFhirLogicalId(fhirLogicalId);
  }

  /**
   * Delete FHIR Procedure resources from OMOP CDM tables using fhir_identifier
   *
   * @param fhirIdentifier identifier of the FHIR Procedure resource
   */
  public void deleteExistingProcedureOccByFhirIdentifier(String fhirIdentifier) {
    procedureOccRepository.deleteByFhirIdentifier(fhirIdentifier);
  }
}
