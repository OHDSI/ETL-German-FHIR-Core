package org.miracum.etl.fhirtoomop.mapper.helpers;

import java.time.LocalDate;
import java.time.Year;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.miracum.etl.fhirtoomop.model.omop.Concept;

class ConceptDictionaryTest {

  @ParameterizedTest
  @CsvSource({
    "2000-01-01, 2019-12-31, 2020, false",
    "2000-01-01, 2019-12-31, 2019, true",
    "2020-01-01, 2021-01-01, 2020, true"
  })
  void getConcept(
      LocalDate conceptValidStart,
      LocalDate conceptValidEnd,
      Year codeVersion,
      boolean expectsConceptIsFound) {
    var concept =
        Concept.builder()
            .validStartDate(conceptValidStart)
            .validEndDate(conceptValidEnd)
            .vocabularyId("ICD10GM")
            .conceptCode("K00.0")
            .build();
    List<Concept> concepts = new ArrayList<>();
    concepts.add(concept);
    //    var sut = new ConceptDictionarys(concepts);
    //
    //    var result = sut.getConcept("K00.0", codeVersion);
    //
    //    if (expectsConceptIsFound) {
    //      assertThat(result).isNotNull();
    //    } else {
    //      assertThat(result).isNull();
    //    }
  }
}
