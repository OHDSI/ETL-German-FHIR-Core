package org.miracum.etl.fhirtoomop.repository;

import org.miracum.etl.fhirtoomop.model.omop.Provider;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;
/**
 * {@code ProviderRepository} is a Spring Data repository interface for CRUD operations on Provider entities.
 * It extends {@link org.springframework.data.jpa.repository.JpaRepository JpaRepository} and
 * {@link org.springframework.data.repository.PagingAndSortingRepository PagingAndSortingRepository}.
 */
@Repository
public interface ProviderRepository
        extends PagingAndSortingRepository<Provider, Long>,
        JpaRepository<Provider, Long> {

        }