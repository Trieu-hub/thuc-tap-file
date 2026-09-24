# One Dockerfile for all three services: docker-compose passes the module name as SERVICE.
# Build context is the repository root (the reactor needs the root pom and the contracts module).

FROM maven:3.9-eclipse-temurin-21 AS build
ARG SERVICE
WORKDIR /src
COPY pom.xml .
COPY contracts contracts
COPY order-service/pom.xml order-service/
COPY payment-service/pom.xml payment-service/
COPY policy-service/pom.xml policy-service/
COPY ${SERVICE}/src ${SERVICE}/src
# The cache mount keeps ~/.m2 between builds (F29); "locked" stops parallel builds corrupting it.
RUN --mount=type=cache,target=/root/.m2,sharing=locked \
    mvn -B -q -pl ${SERVICE} -am package -DskipTests

FROM eclipse-temurin:21-jre
ARG SERVICE
WORKDIR /app
COPY --from=build /src/${SERVICE}/target/${SERVICE}.jar app.jar
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
