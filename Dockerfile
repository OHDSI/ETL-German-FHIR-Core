FROM gradle:7.4.1-jdk17 AS build
WORKDIR /home/gradle/src
ENV GRADLE_USER_HOME /gradle

COPY build.gradle settings.gradle gradle.properties ./
RUN gradle clean build --no-daemon || true

COPY --chown=gradle:gradle . .
RUN gradle build --info && \
    gradle jacocoTestReport && \
    awk -F"," '{ instructions += $4 + $5; covered += $5 } END { print covered, "/", instructions, " instructions covered"; print 100*covered/instructions, "% covered" }' build/reports/jacoco/test/jacocoTestReport.csv && \
    java -Djarmode=layertools -jar build/libs/*.jar extract

FROM gcr.io/distroless/java17-debian11@sha256:6c07dc702666ef34151430725224dc58d9e617d3a88d961335373ac961266b1e
WORKDIR /opt/omoptofhir

COPY --from=build /home/gradle/src/dependencies/ ./
COPY --from=build /home/gradle/src/spring-boot-loader/ ./
COPY --from=build /home/gradle/src/snapshot-dependencies/ ./
COPY --from=build /home/gradle/src/application/ .

USER 65532
ARG VERSION=0.0.0
ENV APP_VERSION=${VERSION} \
    SPRING_PROFILES_ACTIVE=prod
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=85", "-XX:G1PeriodicGCInterval=1000", "-XX:G1PeriodicGCSystemLoadThreshold=1000", "org.springframework.boot.loader.JarLauncher"]

ARG GIT_REF=""
ARG BUILD_TIME=""
