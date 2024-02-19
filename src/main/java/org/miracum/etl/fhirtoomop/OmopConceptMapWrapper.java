package org.miracum.etl.fhirtoomop;

import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ATC;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_ICD10GM;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_IPRD;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_LOINC;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_OPS;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_SNOMED;
import static org.miracum.etl.fhirtoomop.Constants.VOCABULARY_UCUM;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.miracum.etl.fhirtoomop.model.omop.Concept;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class OmopConceptMapWrapper {
  private Map<String, List<Concept>> findValidLoincConcept;
  private Map<String, List<Concept>> findValidUcumConcept;
  private Map<String, List<Concept>> findValidAtcConcept;
  private Map<String, List<Concept>> findValidOpsConcept;
  private Map<String, List<Concept>> findValidSnomedConcept;
  private Map<String, List<Concept>> findValidIcd10GmConcept;
  private Map<String, List<Concept>> findValidIPRDConcept;

  public Map<String, List<Concept>> getValidConcepts(String vocabularyId) {
    switch (vocabularyId) {
      case VOCABULARY_OPS:
        return findValidOpsConcept;
      case VOCABULARY_SNOMED:
        return findValidSnomedConcept;
      case VOCABULARY_ATC:
        return findValidAtcConcept;
      case VOCABULARY_IPRD:
        return findValidIPRDConcept;
      case VOCABULARY_UCUM:
        return findValidUcumConcept;
      case VOCABULARY_ICD10GM:
        return findValidIcd10GmConcept;
      case VOCABULARY_LOINC:
        return findValidLoincConcept;
      default:
        return Collections.emptyMap();
    }
  }
}
