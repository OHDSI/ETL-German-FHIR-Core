package org.miracum.etl.fhirtoomop;

import java.text.NumberFormat;
import lombok.extern.slf4j.Slf4j;

/**
 * The MemoryLogging class is used for the formatted output of memory logging.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class MemoryLogger {
  private String unit = "MB\n";

  /** Formats the representation of the memory logging. */
  public void logMemoryDebugOnly() {

    var runtime = Runtime.getRuntime();
    var format = NumberFormat.getInstance();

    var sb = new StringBuilder();
    long maxMemory = runtime.maxMemory();
    long allocatedMemory = runtime.totalMemory();
    long freeMemory = runtime.freeMemory();

    sb.append("free memory: " + format.format(freeMemory / (1024 * 1024)) + unit);
    sb.append("allocated memory: " + format.format(allocatedMemory / (1024 * 1024)) + unit);
    sb.append("max memory: " + format.format(maxMemory / (1024 * 1024)) + unit);
    sb.append(
        "total free memory: "
            + format.format((freeMemory + (maxMemory - allocatedMemory)) / (1024 * 1024))
            + unit);

    log.debug("MEMORY: {}", sb);
  }
}
