package org.miracum.etl.fhirtoomop.config;

import io.jaegertracing.internal.propagation.TraceContextCodec;
import io.opentracing.contrib.java.spring.jaeger.starter.TracerBuilderCustomizer;
import io.opentracing.propagation.Format;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingConfig {
  @Bean
  public TracerBuilderCustomizer traceContextJaegerTracerCustomizer() {
    return builder -> {
      var injector = new TraceContextCodec.Builder().build();

      builder
          .registerInjector(Format.Builtin.HTTP_HEADERS, injector)
          .registerExtractor(Format.Builtin.HTTP_HEADERS, injector);

      builder
          .registerInjector(Format.Builtin.TEXT_MAP, injector)
          .registerExtractor(Format.Builtin.TEXT_MAP, injector);
    };
  }

  @Bean
  @ConditionalOnProperty(
      value = "opentracing.jaeger.enabled",
      havingValue = "false",
      matchIfMissing = false)
  public io.opentracing.Tracer jaegerTracer() {
    return io.opentracing.noop.NoopTracerFactory.create();
  }
}
