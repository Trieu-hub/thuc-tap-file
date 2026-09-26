# One Dockerfile for all three services: docker-compose passes the module name as SERVICE.
# Build context is the repository root (the reactor needs the root pom and the contracts module).
#
# The build stage does not use SERVICE, so it is identical for the three images: BuildKit runs it
# once and the three images only differ in the jar they copy. Before, each image ran its own
# Maven build of contracts + one service, one after the other because of the cache lock
# (about 8.7 min for the three images).

FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
COPY contracts contracts
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/
COPY policy-service/pom.xml policy-service/
COPY order-service/src order-service/src
COPY payment-service/src payment-service/src
COPY policy-service/src policy-service/src
# The cache mount keeps ~/.m2 between builds (F29); "locked" stops parallel builds corrupting it.
# -T 1C builds the three services in parallel once contracts is done. Not quiet: a compilation
# error must show up in the docker compose output, not only "exit code: 1".
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -ntp -T 1C package -DskipTests

FROM eclipse-temurin:21-jre
ARG SERVICE
WORKDIR /app
COPY --from=build /src/${SERVICE}/target/${SERVICE}.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
