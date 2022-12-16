package org.miracum.etl.fhirtoomop;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;

/**
 * The main class for starting the application
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@SpringBootApplication
@EnableCaching
public class FhirToOmopApplication {
  public static void main(String[] args) {
    System.exit(SpringApplication.exit(SpringApplication.run(FhirToOmopApplication.class, args)));
  }
}
