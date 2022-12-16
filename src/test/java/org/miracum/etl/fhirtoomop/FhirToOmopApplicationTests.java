package org.miracum.etl.fhirtoomop;

import org.junit.jupiter.api.Test;
import org.miracum.etl.fhirtoomop.config.FhirConfig;
import org.miracum.etl.fhirtoomop.config.FhirSystems;
import org.miracum.etl.fhirtoomop.config.TracingConfig;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest(classes = {FhirConfig.class, TracingConfig.class})
@ActiveProfiles("test")
@EnableAutoConfiguration
@EnableConfigurationProperties(value = {FhirSystems.class})
class FhirToOmopApplicationTests {

  @Test
  void contextLoads() {}
}
