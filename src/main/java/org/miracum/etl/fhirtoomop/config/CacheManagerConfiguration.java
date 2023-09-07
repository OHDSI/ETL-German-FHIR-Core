package org.miracum.etl.fhirtoomop.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Arrays;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The CacheManagerConfiguration class configures a CacheManager using CaffeineCache.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Configuration
public class CacheManagerConfiguration {
  @Value("${spring.cache.caffeine.spec.maximumSize}")
  private Long caffeineCacheMaxSize;
  /**
   * Initialize a CaffeineCacheManager.
   *
   * @return a new CaffeineCacheManager
   */
  @Bean(value = "caffeineCacheManager")
  public CaffeineCacheManager cacheManager() {
    CaffeineCacheManager cacheManager = new CaffeineCacheManager();
    cacheManager.setCacheNames(
        Arrays.asList(
            "valid-concepts",
            "icd-snomed",
            "domains",
            "patients-logicalid",
            "patients-identifier",
            "visits-logicalid",
            "visits-identifier",
            "max-id",
            "snomed-vaccine",
            "snomed-race",
            "ops-standard",
            "orpha-snomed",
            "atc-standard",
            "loinc-standard"));
    cacheManager.setCaffeine(caffeineCacheBuilder());
    return cacheManager;
  }

  /**
   * Initialize a CaffeineCache.
   *
   * @return a new CaffeineCache
   */
  private Caffeine<Object, Object> caffeineCacheBuilder() {
    return Caffeine.newBuilder().maximumSize(caffeineCacheMaxSize).softValues().recordStats();
  }
}
