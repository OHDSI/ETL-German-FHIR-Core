package org.miracum.etl.fhirtoomop.utils;

import com.google.common.base.Strings;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.StepContribution;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

/**
 * The class ExecuteSqlScripts executes the SQL scripts and reports the error messages if any.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Slf4j
public class ExecuteSqlScripts {
  private final AtomicInteger sqlRowcounts = new AtomicInteger(0);
  private final DataSource dataSource;
  private final StepContribution contribution;

  /**
   * Constructor for objects of the class ExecuteSqlScripts.
   *
   * @param dataSource the data source to query against
   * @param contribution buffers changes until they can be applied to a chunk boundary
   */
  public ExecuteSqlScripts(DataSource dataSource, StepContribution contribution) {
    this.dataSource = dataSource;
    this.contribution = contribution;
  }

  /**
   * Reads and executes SQL files.
   *
   * @param sqlResourceFile path of the SQL file
   * @throws SQLException
   */
  public void executeSQLScript(Resource sqlResourceFile) throws SQLException, IOException {
    var curCount = 1;

    try (Connection conn = dataSource.getConnection();
        Statement stat = conn.createStatement();
        BufferedReader in =
            new BufferedReader(
                new InputStreamReader(
                    new FileInputStream(sqlResourceFile.getFile()), StandardCharsets.UTF_8)); ) {
      conn.setAutoCommit(true);
      var sqlFile = sqlResourceFile.getFile();
      log.info(
          "==== Executing SQL script: {}. Please be patient, this may take a while ====",
          sqlFile.getName());
      var stepName = contribution.getStepExecution().getStepName();

      if (stepName.equals("stepPostProcess") || stepName.equals("initJobInfo")) {

        executePlpgSqlScripts(stat, in);
      } else {
        executeSimpleSqlScripts(conn, sqlResourceFile);
      }
    }

    //    } catch (IOException e) {
    //      log.error("No File found for {}", sqlResourceFile.getFilename());
    //
    //    } catch (SQLException e) {
    //      log.error("Failed at SQL statement [{}] !", curCount);
    //      log.error(e.getMessage());
    //      log.error("Error State: {}", e.getSQLState());
    //      log.error("Error Code: {}", e.getErrorCode());
    //      throw new SQLException("Failed when running SQL script!");
    //    } catch (Exception e) {
    //      e.printStackTrace();
    //    }
  }

  /**
   * Retrieve numbers from a String
   *
   * @param warningMessage String, from which the numbers are retrieved
   * @return the numbers in the String
   */
  private String retrieveRowCounts(String warningMessage) {

    warningMessage = warningMessage.replaceAll("[^\\d]", "");
    warningMessage = warningMessage.trim();
    return warningMessage;
  }

  /**
   * Executes PL/pgSQL scripts
   *
   * @param stat SQL statements
   * @param in bufferedReader
   * @throws IOException
   * @throws SQLException
   */
  public void executePlpgSqlScripts(Statement stat, BufferedReader in)
      throws IOException, SQLException {
    var sqlCode = "";
    var sql = "";
    var sb = new StringBuilder();
    var blockCharCount = 0;

    while ((sql = in.readLine()) != null) {
      sb.append(sql);
      sb.append("\n");
      if (sql.contains("$$") && blockCharCount < 2) {
        blockCharCount += 1;
      }

      if (blockCharCount == 2) {
        sqlCode = sb.toString();
        stat.execute(sqlCode);

        sb = new StringBuilder();
        var warnings = stat.getWarnings();
        while (warnings != null) {
          log.info("{}", warnings.getMessage());
          var updatedRowCount = retrieveRowCounts(warnings.getMessage());
          if (!Strings.isNullOrEmpty(updatedRowCount)) {
            sqlRowcounts.addAndGet(Integer.parseInt(updatedRowCount));
          }
          warnings = warnings.getNextWarning();
        }
        blockCharCount = 0;
      }
    }

    contribution.incrementWriteCount(sqlRowcounts.get());
    sqlRowcounts.set(0);
  }
  /**
   * Execute SQL scripts using ScriptUtils
   *
   * @param conn connection information of database
   * @param sqlResourceFile to be executed SQL file
   */
  private void executeSimpleSqlScripts(Connection conn, Resource sqlResourceFile) {
    ScriptUtils.executeSqlScript(conn, sqlResourceFile);
  }
}
