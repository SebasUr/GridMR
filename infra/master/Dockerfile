FROM maven:3.9.6-eclipse-temurin-17 AS build
WORKDIR /src
COPY . /src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /src/target/gridmr-master*.jar /app/gridmr-master.jar
EXPOSE 50051
ENV MASTER_PORT=50051
CMD ["java", "-jar", "gridmr-master.jar"]
