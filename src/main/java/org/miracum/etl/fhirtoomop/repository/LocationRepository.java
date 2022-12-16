package org.miracum.etl.fhirtoomop.repository;

import org.miracum.etl.fhirtoomop.model.omop.Location;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * The LocationRepository interface represents a repository for the location table in OMOP CDM.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
public interface LocationRepository
    extends PagingAndSortingRepository<Location, Long>, JpaRepository<Location, Long> {}
