package org.miracum.etl.fhirtoomop;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Getter;

/**
 * A summary of ConcurrentHashMaps used for in-memory mapping.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Getter
public class InMemoryIncrementalIdMappings implements IIdMappings {
  private final IdMapping<String, Long> persons = new InMemoryIdMapping();
  private final IdMapping<String, Long> locations = new InMemoryIdMapping();
  private final IdMapping<String, Long> visitDetailIds = new InMemoryIdMapping();
  private final IdMapping<String, Long> visitOccId = new InMemoryIdMapping();
  private final IdMapping<String, Long> drugExposureIds = new InMemoryIdMapping();
  private final IdMapping<String, Long> medicationIds = new InMemoryIdMapping();

  /**
   * The InMemoryIdMapping class describes the methods for managing the caching of key-value
   * relationships.
   *
   * @author Elisa Henke
   * @author Yuan Peng
   */
  public static class InMemoryIdMapping implements IdMapping<String, Long> {
    private final AtomicLong counter = new AtomicLong(1);
    private final ConcurrentHashMap<String, Long> map = new ConcurrentHashMap<>();

    @Override
    public Long getOrCreateIfAbsent(String key) {
      return map.computeIfAbsent(key, k -> counter.getAndIncrement());
    }

    @Override
    public Long get(String key) {
      return map.get(key);
    }

    @Override
    public Long getOrDefault(String key, Long defaultValue) {
      return map.getOrDefault(key, defaultValue);
    }

    @Override
    public Long put(String key, Long value) {
      return map.put(key, value);
    }

    @Override
    public void clear() {
      map.clear();
    }

    @Override
    public boolean containsKey(String s) {
      return map.containsKey(s);
    }
  }
}
