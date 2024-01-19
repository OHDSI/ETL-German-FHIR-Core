package org.miracum.etl.fhirtoomop.config;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.fhirpath.IFhirPath;
import ca.uhn.fhir.parser.IParser;
import ca.uhn.fhir.rest.client.api.IClientInterceptor;
import ca.uhn.fhir.rest.client.api.IGenericClient;
import ca.uhn.fhir.rest.client.interceptor.BasicAuthInterceptor;
import ca.uhn.fhir.rest.client.interceptor.BearerTokenAuthInterceptor;
import com.google.common.base.Strings;
import org.hl7.fhir.r4.hapi.fluentpath.FhirPathR4;
import org.miracum.etl.fhirtoomop.mapper.helpers.ResourceFhirReferenceUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Configures all FHIR settings so that all FHIR resources can be processed.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Configuration
public class FhirConfig {
  @Value("${data.fhirServer.baseUrl}")
  private String fhirBaseUrl;

  @Value("${data.fhirServer.connectionTimeout}")
  private Integer connectionTimeout;

  @Value("${data.fhirServer.socketTimeout}")
  private Integer socketTimeout;

  @Value("${data.fhirServer.username}")
  private String fhirServerUsername;

  @Value("${data.fhirServer.password}")
  private String fhirServerPassword;

  @Value("${data.fhirServer.token}")
  private String fhirServerToken;
  /**
   * Sets the FHIR context with FHIR R4 version.
   *
   * @return FhirContext for R4 version of FHIR
   */
  @Bean
  public FhirContext fhirContext() {
    return FhirContext.forR4();
  }

  /**
   * Creates a parser, which can parse a FHIR resource in JSON format.
   *
   * @param ctx the central starting point for the use of the HAPI FHIR API. It should be created
   *     once, and then used as a factory for various other types of objects (parsers, clients,
   *     etc.).
   * @return a new JsonParser
   */
  @Bean
  public IParser fhirParser(FhirContext ctx) {
    return ctx.newJsonParser().setPrettyPrint(false);
  }

  /**
   * Enables path-based navigation and extraction for FHRI R4 version.
   *
   * @param ctx ctx the central starting point for the use of the HAPI FHIR API. It should be
   *     created once, and thenused as a factory for various other types of objects (parsers,
   *     clients, etc.).
   * @return a new IFhirPath for FHIR R4 version
   */
  @Bean
  public IFhirPath fhirPath(FhirContext ctx) {
    return new FhirPathR4(ctx);
  }

  /**
   * Initializes the ResourceFhirReferenceUtils to extract references to other FHIR resources from
   * the processing FHIR resources.
   *
   * @param fhirSystems references to naming and coding systems used in FHIR resources
   * @param fhirPath FhirPath engine to evaluate path expressions over FHIR resources
   * @return a new ResourceFhirReferenceUtils
   */
  @Bean
  public ResourceFhirReferenceUtils resourceReferenceUtils(
      FhirSystems fhirSystems, IFhirPath fhirPath) {
    return new ResourceFhirReferenceUtils(fhirPath, fhirSystems);
  }

  @Bean
  public IGenericClient client(FhirContext fhirContext) {
    fhirContext.getRestfulClientFactory().setSocketTimeout(socketTimeout);
    fhirContext.getRestfulClientFactory().setConnectTimeout(connectionTimeout);
    var fhirClient = fhirContext.newRestfulGenericClient(fhirBaseUrl);
    if (!Strings.isNullOrEmpty(fhirServerPassword) && !Strings.isNullOrEmpty(fhirServerUsername)) {
      IClientInterceptor authInterceptor =
          new BasicAuthInterceptor(fhirServerUsername, fhirServerPassword);
      fhirClient.registerInterceptor(authInterceptor);
    }

    if(!Strings.isNullOrEmpty(fhirServerToken)) {
      IClientInterceptor authInterceptor =
              new BearerTokenAuthInterceptor(fhirServerToken);
      fhirClient.registerInterceptor(authInterceptor);
    }

    //    LoggingInterceptor loggingInterceptor = new LoggingInterceptor();
    //    loggingInterceptor.setLogRequestSummary(true);
    //
    //    loggingInterceptor.setLogResponseSummary(true);
    //
    //    fhirClient.registerInterceptor(loggingInterceptor);

    return fhirClient;
  }
}
