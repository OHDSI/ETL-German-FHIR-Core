package org.miracum.etl.fhirtoomop.mapper.helpers;

import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * The ResourceOnset class represents the start date and time and the end date and time of a
 * clinical event.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public final class ResourceOnset {
  private LocalDateTime startDateTime;
  private LocalDateTime endDateTime;

  /**
   * Constructor for objects of the class ResourceOnset.
   *
   * @param startDateTime the start date and time of a clinical event
   * @param endDateTime the end date and time of a clinical event
   */
  //  public ResourceOnset(LocalDateTime startDateTime, LocalDateTime endDateTime) {
  //    this.startDateTime = startDateTime;
  //    this.endDateTime = endDateTime;
  //  }
}
