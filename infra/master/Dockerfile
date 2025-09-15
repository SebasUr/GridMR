FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /src
COPY . /src
RUN mvn -q -DskipTests -DskipITs package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/gridmr-master.jar /app/gridmr-master.jar
EXPOSE 50051
EXPOSE 8080
ENV MASTER_PORT=50051
ENV MASTER_HTTP_PORT=8080
ENTRYPOINT ["java","-jar","/app/gridmr-master.jar"]
