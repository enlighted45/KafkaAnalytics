# syntax=docker/dockerfile:1
# Build context: repository root (needs the parent pom and txn-common).
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY txn-common/pom.xml txn-common/
COPY producer-service/pom.xml producer-service/
COPY analytics-service/pom.xml analytics-service/
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B -pl analytics-service -am dependency:go-offline -DskipTests || true
COPY txn-common txn-common
COPY analytics-service analytics-service
RUN --mount=type=cache,target=/root/.m2 ./mvnw -q -B -pl analytics-service -am package -DskipTests

FROM eclipse-temurin:21-jre
RUN groupadd --system app && useradd --system --gid app --home /app app \
    && mkdir -p /var/lib/streams && chown app:app /var/lib/streams
WORKDIR /app
COPY --from=build /src/analytics-service/target/analytics-service.jar app.jar
USER app
EXPOSE 8090
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+ExitOnOutOfMemoryError"
HEALTHCHECK --interval=10s --timeout=5s --start-period=40s --retries=6 \
  CMD bash -c 'exec 3<>/dev/tcp/127.0.0.1/8090 && printf "GET /actuator/health/readiness HTTP/1.0\r\n\r\n" >&3 && grep -q "UP" <&3'
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
