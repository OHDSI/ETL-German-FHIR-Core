package org.miracum.etl.fhirtoomop;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.Database;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.Properties;

/**
 * Configures all database connections that will be used in this job.
 *
 * @author Elisa Henke
 * @author Yuan Peng
 */
@Configuration
// @EnableTransactionManagement
@EnableJpaRepositories(basePackages = "org.miracum.etl.fhirtoomop.repository")
public class DataSourceConfig {

  /**
   * Configures a default data source.
   *
   * @return default data source
   */
  @Bean
  @Primary
  public DataSource defaultDataSource() {
    var embeddedDatabaseBuilder = new EmbeddedDatabaseBuilder();

    return embeddedDatabaseBuilder
        .addScript("H2CompatibilityMode.sql")
        .addScript("classpath:org/springframework/batch/core/schema-drop-h2.sql")
        .addScript("classpath:org/springframework/batch/core/schema-h2.sql")
        .setType(EmbeddedDatabaseType.H2)
        .build();
  }

  /**
   * Configures the source database connection.
   *
   * @return source database connection
   */
  @Bean
  @Qualifier("readerDataSource")
  @ConfigurationProperties(prefix = "data.fhir-gateway")
  public DataSource readerDataSource() {
    return DataSourceBuilder.create().build();
  }

  /**
   * Configures the target database connection.
   *
   * @return target database connection
   */
  @Bean
  @Qualifier("writerDataSource")
  @ConfigurationProperties(prefix = "data.omop-cdm")
  public DataSource writerDataSource() {
    return DataSourceBuilder.create().build();
  }

  /**
   * Configures a JdbcTemplate for target database.
   *
   * @param dataSource database connection of target database
   * @return a JdbcTemplate for target database
   */
  @Bean
  @Primary
  @Qualifier("writerJdbcTemplate")
  public JdbcTemplate jdbcTemplate(@Qualifier("writerDataSource") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  /**
   * Configures a JdbcTemplate for source database.
   *
   * @param dataSource database connection of source database
   * @return a JdbcTemplate for source database
   */
  @Bean
  @Qualifier("readerJdbcTemplate")
  public JdbcTemplate readerJdbcTemplate(@Qualifier("readerDataSource") DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  /**
   * Configures a EntityManagerFactory to be able to use SpringJPA.
   *
   * @return entity manager factory for target database tables
   */
  @Bean
  public EntityManagerFactory entityManagerFactory() {
    var vendorAdapter = new HibernateJpaVendorAdapter();
    vendorAdapter.setDatabase(Database.POSTGRESQL);
    vendorAdapter.setGenerateDdl(true);
    var factory = new LocalContainerEntityManagerFactoryBean();
    factory.setJpaVendorAdapter(vendorAdapter);
    factory.setPackagesToScan("org.miracum.etl.fhirtoomop.model");
    factory.setDataSource(writerDataSource());
    Properties properties = new Properties();
    properties.put("hibernate.default_schema", "cds_cdm");
    factory.setJpaProperties(properties);
    factory.afterPropertiesSet();
    return factory.getObject();
  }

  /**
   * Configures a PlatformTransactionManager to be able to use SpringJPA.
   *
   * @return platform transaction manager for target database
   */
  @Bean
  public PlatformTransactionManager transactionManager() {
    var txManager = new JpaTransactionManager();
    txManager.setEntityManagerFactory(entityManagerFactory());
    txManager.setDataSource(writerDataSource());
    return txManager;
  }
}
