package org.miracum.etl.fhirtoomop;

/**
 * The interface IIdMappings is used to cache manually created ids for OMOP CDM tables.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface IIdMappings {

  IdMapping<String, Long> getPersons();

  IdMapping<String, Long> getMedicationIds();

  /**
   * The IdMapping interface describes the possible methods for managing the caching of key-value
   * relationships.
   *
   * @param <TKey> the unique key
   * @param <TValue> the value belonging to the unique key
   */
  interface IdMapping<TKey, TValue> {
    TValue getOrCreateIfAbsent(TKey key);

    TValue get(String TKey);

    TValue getOrDefault(String TKey, TValue defaultValue);

    TValue put(TKey key, TValue value);

    void clear();

    boolean containsKey(TKey key);
  }
}
